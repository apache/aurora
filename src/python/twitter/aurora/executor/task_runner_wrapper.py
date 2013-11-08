import errno
import getpass
import os
import signal
import subprocess
import tempfile
import threading
import time

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.dirutil import chmod_plus_x
from twitter.common.quantity import Amount, Time

from twitter.thermos.common.path import TaskPath
from twitter.thermos.config.loader import ThermosTaskWrapper
from twitter.thermos.core.runner import TaskRunner
from twitter.thermos.monitoring.monitor import TaskMonitor

from .common.sandbox import SandboxInterface

from gen.twitter.thermos.ttypes import TaskState


app.add_option("--checkpoint_root", dest="checkpoint_root", metavar="PATH",
               default=TaskPath.DEFAULT_CHECKPOINT_ROOT,
               help="the path where we will store workflow logs and checkpoints")


class TaskRunnerWrapper(object):
  PEX_NAME = 'thermos_runner.pex'
  POLL_INTERVAL = Amount(500, Time.MILLISECONDS)

  class TaskError(Exception):
    pass

  def __init__(self, task_id, mesos_task, role, mesos_ports, runner_pex, sandbox,
               checkpoint_root=None, artifact_dir=None, clock=time):
    """
      :task_id     => task_id assigned by scheduler
      :mesos_task  => twitter.aurora.config.schema.MesosTaskInstance object
      :mesos_ports => { name => port } dictionary
    """
    self._popen = None
    self._monitor = None
    self._dead = threading.Event()
    self._task_id = task_id
    self._mesos_task = mesos_task
    self._task = mesos_task.task()
    self._ports = mesos_ports
    self._runner_pex = runner_pex
    if not os.path.exists(self._runner_pex):
      raise self.TaskError('Specified runner pex:%s does not exist!' % self._runner_pex)
    self._sandbox = sandbox
    if not isinstance(self._sandbox, SandboxInterface):
      raise ValueError('sandbox must be derived from SandboxInterface!')
    self._checkpoint_root = checkpoint_root or app.get_options().checkpoint_root
    self._enable_chroot = False
    self._role = role
    self._clock = clock
    self._artifact_dir = artifact_dir or tempfile.mkdtemp()
    self._kill_signal = threading.Event()

    try:
      with open(os.path.join(self._artifact_dir, 'task.json'), 'w') as fp:
        self._task_filename = fp.name
        ThermosTaskWrapper(self._task).to_file(self._task_filename)
    except ThermosTaskWrapper.InvalidTask as e:
      raise self.TaskError('Failed to load task: %s' % e)

  @property
  def pid(self):
    return self._popen.pid if self._popen else None

  @property
  def artifact_dir(self):
    return self._artifact_dir

  @property
  def sandbox(self):
    return self._sandbox

  def initialize(self):
    """
      Initialize the sandbox for the task runner. Depending on the implementation, this may take
      some time to complete.
    """
    try:
      log.info('Creating sandbox.')
      self._sandbox.create(self._mesos_task)
    except Exception as e:
      log.fatal('Could not construct sandbox: %s' % e)
      raise self.TaskError('Could not construct sandbox: %s' % e)

  def start(self):
    """
      Fork the task runner.
    """
    if not self.is_initialized():
      raise self.TaskError('Cannot start task runner before initialization')
    chmod_plus_x(self._runner_pex)
    self._monitor = TaskMonitor(TaskPath(root=self._checkpoint_root), self._task_id)

    params = dict(log_dir=LogOptions.log_dir(),
                  log_to_disk="DEBUG",
                  checkpoint_root=self._checkpoint_root,
                  sandbox=self._sandbox.root,
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
    log.info('Forking off runner with cmdline: %s' % ' '.join(cmdline_args))
    try:
      self._popen = subprocess.Popen(cmdline_args)
    except OSError as e:
      raise self.TaskError(e)

  def state(self):
    return self._monitor.get_state() if self._monitor else None

  def task_state(self):
    return self._monitor.task_state() if self._monitor else None

  def is_initialized(self):
    return self._sandbox.exists()

  def is_started(self):
    return self._monitor and (self._monitor.active or self._monitor.finished)

  def is_alive(self):
    """
      Is the process underlying the Thermos task runner alive?
    """
    if not self.is_started():
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
    except OSError as e:
      if e.errno != errno.ECHILD:
        raise

    self._dead.set()
    return False

  def cleanup(self):
    # For subclasses to implement.  Will be called by the status manager on
    # runner termination.
    pass

  def terminate_runner(self, as_loss=False):
    """
      Terminate the underlying runner process, if it exists.
    """
    if self._kill_signal.is_set():
      log.warning('Duplicate kill/lose signal received, ignoring.')
      return
    self._kill_signal.set()
    if self.is_alive():
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
      runner = TaskRunner.get(self._task_id, self._checkpoint_root)
      if runner:
        log.info('quitquitquit calling runner.kill')
        runner.kill(force=True)
      else:
        log.error('Could not instantiate runner!')
    except TaskRunner.Error as e:
      log.error('Could not quitquitquit runner: %s' % e)
