import errno
import getpass
import os
import signal
import subprocess
import threading
import time

from twitter.aurora.common.http_signaler import HttpSignaler
from twitter.common import log
from twitter.common.dirutil import chmod_plus_x, safe_mkdtemp
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time
from twitter.thermos.common.path import TaskPath
from twitter.thermos.config.loader import ThermosTaskWrapper
from twitter.thermos.core import runner as core
from twitter.thermos.monitoring.monitor import TaskMonitor

from gen.twitter.thermos.ttypes import TaskState

from .common.status_checker import ExitState, StatusResult
from .common.task_info import (
    mesos_task_instance_from_assigned_task,
    resolve_ports,
)
from .common.task_runner import (
    TaskError,
    TaskRunner,
    TaskRunnerProvider,
)


class ThermosTaskRunner(TaskRunner):
  ESCALATION_WAIT = Amount(5, Time.SECONDS)
  EXIT_STATE_MAP = {
      TaskState.ACTIVE: StatusResult('Runner died while task was active.', ExitState.LOST),
      TaskState.FAILED: StatusResult('Task failed.', ExitState.FAILED),
      TaskState.KILLED: StatusResult('Task killed.', ExitState.KILLED),
      TaskState.LOST: StatusResult('Task lost.', ExitState.LOST),
      TaskState.SUCCESS: StatusResult('Task finished.', ExitState.FINISHED),
  }
  MAX_WAIT = Amount(1, Time.MINUTES)
  PEX_NAME = 'thermos_runner.pex'
  POLL_INTERVAL = Amount(500, Time.MILLISECONDS)
  THERMOS_PREEMPTION_WAIT = Amount(1, Time.MINUTES)

  def __init__(self,
               runner_pex,
               task_id,
               task,
               role,
               portmap,
               sandbox,
               checkpoint_root=None,
               artifact_dir=None,
               clock=time):
    """
      runner_pex       location of the thermos_runner pex that this task runner should use
      task_id          task_id assigned by scheduler
      task             thermos pystachio Task object
      role             role to run the task under
      portmap          { name => port } dictionary
      sandbox          the sandbox object
      checkpoint_root  the checkpoint root for the thermos runner
      artifact_dir     scratch space for the thermos runner (basically cwd of thermos.pex)
      clock            clock
    """
    self._runner_pex = runner_pex
    self._task_id = task_id
    self._task = task
    self._popen = None
    self._monitor = None
    self._status = None
    self._ports = portmap
    self._root = sandbox.root
    self._checkpoint_root = checkpoint_root or TaskPath.DEFAULT_CHECKPOINT_ROOT
    self._enable_chroot = sandbox.chrooted
    self._role = role
    self._clock = clock
    self._artifact_dir = artifact_dir or safe_mkdtemp()

    # wait events
    self._dead = threading.Event()
    self._kill_signal = threading.Event()
    self.forking = threading.Event()
    self.forked = threading.Event()

    try:
      with open(os.path.join(self._artifact_dir, 'task.json'), 'w') as fp:
        self._task_filename = fp.name
        ThermosTaskWrapper(self._task).to_file(self._task_filename)
    except ThermosTaskWrapper.InvalidTask as e:
      raise TaskError('Failed to load task: %s' % e)

  def _terminate_http(self):
    if 'health' not in self._ports:
      return

    http_signaler = HttpSignaler(self._ports['health'])

    # pass 1
    http_signaler.quitquitquit()
    self._clock.sleep(self.ESCALATION_WAIT.as_(Time.SECONDS))
    if self.status is not None:
      return True

    # pass 2
    http_signaler.abortabortabort()
    self._clock.sleep(self.ESCALATION_WAIT.as_(Time.SECONDS))
    if self.status is not None:
      return True

  @property
  def artifact_dir(self):
    return self._artifact_dir

  def task_state(self):
    return self._monitor.task_state() if self._monitor else None

  @property
  def is_alive(self):
    """
      Is the process underlying the Thermos task runner alive?
    """
    if not self._popen:
      return False
    if self._dead.is_set():
      return False

    # N.B. You cannot mix this code and any code that relies upon os.wait
    # mechanisms with blanket child process collection.  One example is the
    # Thermos task runner which calls os.wait4 -- without refactoring, you
    # should not mix a Thermos task runner in the same process as this
    # thread.
    try:
      pid, _ = os.waitpid(self._popen.pid, os.WNOHANG)
      if pid == 0:
        return True
      else:
        log.info('Detected runner termination: pid=%s' % pid)
    except OSError as e:
      log.error('is_alive got OSError: %s' % e)
      if e.errno != errno.ECHILD:
        raise

    self._dead.set()
    return False

  def compute_status(self):
    if self.is_alive:
      return None
    exit_state = self.EXIT_STATE_MAP.get(self.task_state())
    if exit_state is None:
      log.error('Received unexpected exit state from TaskMonitor.')
    return exit_state

  def terminate_runner(self, as_loss=False):
    """
      Terminate the underlying runner process, if it exists.
    """
    if self._kill_signal.is_set():
      log.warning('Duplicate kill/lose signal received, ignoring.')
      return
    self._kill_signal.set()
    if self.is_alive:
      sig = 'SIGUSR2' if as_loss else 'SIGUSR1'
      log.info('Runner is alive, sending %s' % sig)
      try:
        self._popen.send_signal(getattr(signal, sig))
      except OSError as e:
        log.error('Got OSError sending %s: %s' % (sig, e))
    else:
      log.info('Runner is dead, skipping kill.')

  def kill(self):
    self.terminate_runner()

  def lose(self):
    self.terminate_runner(as_loss=True)

  def quitquitquit(self):
    """Bind to the process tree of a Thermos task and kill it with impunity."""
    try:
      runner = core.TaskRunner.get(self._task_id, self._checkpoint_root)
      if runner:
        log.info('quitquitquit calling runner.kill')
        # Right now preemption wait is hardcoded, though it may become configurable in the future.
        runner.kill(force=True, preemption_wait=self.THERMOS_PREEMPTION_WAIT)
      else:
        log.error('Could not instantiate runner!')
    except core.TaskRunner.Error as e:
      log.error('Could not quitquitquit runner: %s' % e)

  def _cmdline(self):
    params = dict(log_dir=LogOptions.log_dir(),
                  log_to_disk='DEBUG',
                  checkpoint_root=self._checkpoint_root,
                  sandbox=self._root,
                  task_id=self._task_id,
                  thermos_json=self._task_filename)

    if getpass.getuser() == 'root':
      params.update(setuid=self._role)

    cmdline_args = [self._runner_pex]
    cmdline_args.extend('--%s=%s' % (flag, value) for flag, value in params.items())
    if self._enable_chroot:
      cmdline_args.extend(['--enable_chroot'])
    for name, port in self._ports.items():
      cmdline_args.extend(['--port=%s:%s' % (name, port)])
    return cmdline_args

  # --- public interface
  def start(self, timeout=MAX_WAIT):
    """Fork the task runner and return once the underlying task is running, up to timeout."""
    self.forking.set()

    try:
      chmod_plus_x(self._runner_pex)
    except OSError as e:
      if e.errno != errno.EPERM:
        raise TaskError('Failed to chmod +x runner: %s' % e)

    self._monitor = TaskMonitor(TaskPath(root=self._checkpoint_root), self._task_id)

    cmdline_args = self._cmdline()
    log.info('Forking off runner with cmdline: %s' % ' '.join(cmdline_args))

    try:
      self._popen = subprocess.Popen(cmdline_args)
    except OSError as e:
      raise TaskError(e)

    self.forked.set()

    log.debug('Waiting for task to start.')

    def is_started():
      return self._monitor and (self._monitor.active or self._monitor.finished)

    waited = Amount(0, Time.SECONDS)
    while not is_started() and waited < timeout:
      log.debug('  - sleeping...')
      self._clock.sleep(self.POLL_INTERVAL.as_(Time.SECONDS))
      waited += self.POLL_INTERVAL

    if not is_started():
      log.error('Task did not start with in deadline, forcing loss.')
      self.lose()
      raise TaskError('Task did not start within deadline.')

  def stop(self, timeout=MAX_WAIT):
    """Stop the runner.  If it's already completed, no-op.  If it's still running, issue a kill."""
    log.info('ThermosTaskRunner is shutting down.')

    if not self.forking.is_set():
      raise TaskError('Failed to call TaskRunner.start.')

    log.info('Invoking runner HTTP teardown.')
    self._terminate_http()

    log.info('Invoking runner.kill')
    self.kill()

    waited = Amount(0, Time.SECONDS)
    while self.is_alive and waited < timeout:
      self._clock.sleep(self.POLL_INTERVAL.as_(Time.SECONDS))
      waited += self.POLL_INTERVAL

    if not self.is_alive and self.task_state() != TaskState.ACTIVE:
      return

    log.info('Thermos task did not shut down cleanly, rebinding to kill.')
    self.quitquitquit()

    while not self._monitor.finished and waited < timeout:
      self._clock.sleep(self.POLL_INTERVAL.as_(Time.SECONDS))
      waited += self.POLL_INTERVAL

    if not self._monitor.finished:
      raise TaskError('Task did not stop within deadline.')

  @property
  def status(self):
    """Return the StatusResult of this task runner.  This returns None as
       long as no terminal state is reached."""
    if self._status is None:
      self._status = self.compute_status()
    return self._status


class DefaultThermosTaskRunnerProvider(TaskRunnerProvider):
  def __init__(self,
               pex_location,
               checkpoint_root=None,
               artifact_dir=None,
               task_runner_class=ThermosTaskRunner,
               max_wait=Amount(1, Time.MINUTES),
               preemption_wait=Amount(1, Time.MINUTES),
               poll_interval=Amount(500, Time.MILLISECONDS),
               clock=time):
    self._artifact_dir = artifact_dir or safe_mkdtemp()
    self._checkpoint_root = checkpoint_root
    self._clock = clock
    self._max_wait = max_wait
    self._pex_location = pex_location
    self._poll_interval = poll_interval
    self._preemption_wait = preemption_wait
    self._task_runner_class = task_runner_class

  def from_assigned_task(self, assigned_task, sandbox):
    task_id = assigned_task.taskId
    role = assigned_task.task.owner.role
    mesos_task = mesos_task_instance_from_assigned_task(assigned_task)
    mesos_ports = resolve_ports(mesos_task, assigned_task.assignedPorts)

    class ProvidedThermosTaskRunner(self._task_runner_class):
      MAX_WAIT = self._max_wait
      POLL_INTERVAL = self._poll_interval
      THERMOS_PREEMPTION_WAIT = self._preemption_wait

    return ProvidedThermosTaskRunner(
        self._pex_location,
        task_id,
        mesos_task.task(),
        role,
        mesos_ports,
        sandbox,
        checkpoint_root=self._checkpoint_root,
        artifact_dir=self._artifact_dir,
        clock=self._clock)
