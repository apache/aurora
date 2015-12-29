#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import errno
import getpass
import os
import signal
import socket
import struct
import subprocess
import sys
import threading
import time

from mesos.interface import mesos_pb2
from twitter.common import log
from twitter.common.dirutil import safe_mkdtemp
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

from apache.thermos.common.statuses import (
    INTERNAL_ERROR,
    INVALID_TASK,
    TERMINAL_TASK,
    UNKNOWN_ERROR,
    UNKNOWN_USER
)
from apache.thermos.config.loader import ThermosTaskWrapper
from apache.thermos.core import runner as core
from apache.thermos.monitoring.monitor import TaskMonitor

from .common.status_checker import StatusResult
from .common.task_info import mesos_task_instance_from_assigned_task, resolve_ports
from .common.task_runner import TaskError, TaskRunner, TaskRunnerProvider
from .http_lifecycle import HttpLifecycleManager

from gen.apache.thermos.ttypes import TaskState


class ThermosTaskRunner(TaskRunner):
  ESCALATION_WAIT = Amount(5, Time.SECONDS)
  EXIT_STATE_MAP = {
      TaskState.ACTIVE: StatusResult('Runner died while task was active.', mesos_pb2.TASK_LOST),
      TaskState.FAILED: StatusResult('Task failed.', mesos_pb2.TASK_FAILED),
      TaskState.KILLED: StatusResult('Task killed.', mesos_pb2.TASK_KILLED),
      TaskState.LOST: StatusResult('Task lost.', mesos_pb2.TASK_LOST),
      TaskState.SUCCESS: StatusResult('Task finished.', mesos_pb2.TASK_FINISHED),
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
               checkpoint_root,
               artifact_dir=None,
               clock=time,
               hostname=None,
               process_logger_mode=None,
               rotate_log_size_mb=None,
               rotate_log_backups=None,
               preserve_env=False):
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
      preserve_env
    """
    self._runner_pex = runner_pex
    self._task_id = task_id
    self._task = task
    self._popen, self._popen_signal, self._popen_rc = None, None, None
    self._monitor = None
    self._status = None
    self._ports = portmap
    self._root = sandbox.root
    self._checkpoint_root = checkpoint_root
    self._enable_chroot = sandbox.chrooted
    self._preserve_env = preserve_env
    self._role = role
    self._clock = clock
    self._artifact_dir = artifact_dir or safe_mkdtemp()
    self._hostname = hostname or socket.gethostname()
    self._process_logger_mode = process_logger_mode
    self._rotate_log_size_mb = rotate_log_size_mb
    self._rotate_log_backups = rotate_log_backups

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

  @property
  def artifact_dir(self):
    return self._artifact_dir

  def task_state(self):
    return self._monitor.task_state() if self._monitor else None

  @classmethod
  def _decode_status(cls, status):
    # Per os.waitpid documentation, status is:
    #   a 16-bit number, whose low byte is the signal number that killed the
    #   process, and whose high byte is the exit status (if the signal
    #   number is zero); the high bit of the low byte is set if a core file
    #   was produced.
    exit_signal, exit_status = struct.unpack('bb', struct.pack('H', status))
    return exit_signal & 0x7F, exit_status  # strip the signal high bit

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
      pid, status = os.waitpid(self._popen.pid, os.WNOHANG)
      if pid == 0:
        return True
      else:
        self._popen_signal, self._popen_rc = self._decode_status(status)
        log.info('Detected runner termination: pid=%s, signal=%s, rc=%s' % (
            pid, self._popen_signal, self._popen_rc))
    except OSError as e:
      log.error('is_alive got OSError: %s' % e)
      if e.errno != errno.ECHILD:
        raise

    self._dead.set()
    return False

  def compute_status(self):
    if self.is_alive:
      return None
    if self._popen_signal != 0:
      return StatusResult('Task killed by signal %s.' % self._popen_signal, mesos_pb2.TASK_KILLED)
    if self._popen_rc == 0 or self._popen_rc == TERMINAL_TASK:
      exit_state = self.EXIT_STATE_MAP.get(self.task_state())
      if exit_state is None:
        log.error('Received unexpected exit state from TaskMonitor.')
        return StatusResult('Task checkpoint could not be read.', mesos_pb2.TASK_LOST)
      else:
        return exit_state
    elif self._popen_rc == UNKNOWN_USER:
      return StatusResult('Task started with unknown user.', mesos_pb2.TASK_FAILED)
    elif self._popen_rc == INTERNAL_ERROR:
      return StatusResult('Thermos failed with internal error.', mesos_pb2.TASK_LOST)
    elif self._popen_rc == INVALID_TASK:
      return StatusResult('Thermos received an invalid task.', mesos_pb2.TASK_FAILED)
    elif self._popen_rc == UNKNOWN_ERROR:
      return StatusResult('Thermos failed with an unknown error.', mesos_pb2.TASK_LOST)
    else:
      return StatusResult('Thermos exited for unknown reason (exit status: %s)' % self._popen_rc,
          mesos_pb2.TASK_LOST)

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
    host_sandbox = None
    if os.environ.get('MESOS_DIRECTORY'):
      host_sandbox = os.path.join(os.environ.get('MESOS_DIRECTORY'), 'sandbox')

    params = dict(log_dir=LogOptions.log_dir(),
                  log_to_disk='DEBUG',
                  checkpoint_root=self._checkpoint_root,
                  sandbox=host_sandbox or self._root,
                  task_id=self._task_id,
                  thermos_json=self._task_filename,
                  hostname=self._hostname,
                  process_logger_mode=self._process_logger_mode,
                  rotate_log_size_mb=self._rotate_log_size_mb,
                  rotate_log_backups=self._rotate_log_backups)

    if getpass.getuser() == 'root' and self._role:
      params.update(setuid=self._role)

    cmdline_args = [sys.executable, self._runner_pex]
    cmdline_args.extend(
        '--%s=%s' % (flag, value) for flag, value in params.items() if value is not None)
    if self._enable_chroot:
      cmdline_args.extend(['--enable_chroot'])
    if self._preserve_env:
      cmdline_args.extend(['--preserve_env'])
    for name, port in self._ports.items():
      cmdline_args.extend(['--port=%s:%s' % (name, port)])
    return cmdline_args

  # --- public interface
  def start(self, timeout=MAX_WAIT):
    """Fork the task runner and return once the underlying task is running, up to timeout."""
    self.forking.set()

    self._monitor = TaskMonitor(self._checkpoint_root, self._task_id)

    cmdline_args = self._cmdline()
    log.info('Forking off runner with cmdline: %s' % ' '.join(cmdline_args))

    cwd = os.environ.get('MESOS_DIRECTORY')
    try:
      self._popen = subprocess.Popen(cmdline_args, cwd=cwd)
    except OSError as e:
      raise TaskError(e)

    self.forked.set()

    self.wait_start(timeout=timeout)

  def wait_start(self, timeout=MAX_WAIT):
    log.debug('Waiting for task to start.')

    def is_started():
      return self._monitor and (self._monitor.active or self._monitor.finished)

    waited = Amount(0, Time.SECONDS)

    while waited < timeout:
      if not is_started():
        log.debug('  - sleeping...')
        self._clock.sleep(self.POLL_INTERVAL.as_(Time.SECONDS))
        waited += self.POLL_INTERVAL
      else:
        break

      if not self.is_alive:
        if self._popen_rc != 0:
          raise TaskError('Task failed: %s' % self.compute_status().reason)
        else:
          # We can end up here if the process exited between the call to Popen and
          # waitpid (in is_alive), which is fine.
          log.info('Task runner exited: %s' % self.compute_status().reason)
          break

    if not is_started():
      log.error('Task did not start with in deadline, forcing loss.')
      self.lose()
      raise TaskError('Task did not start within deadline.')

  def stop(self, timeout=MAX_WAIT):
    """Stop the runner.  If it's already completed, no-op.  If it's still running, issue a kill."""
    log.info('ThermosTaskRunner is shutting down.')

    if not self.forking.is_set():
      raise TaskError('Failed to call TaskRunner.start.')

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
               checkpoint_root,
               artifact_dir=None,
               preserve_env=False,
               task_runner_class=ThermosTaskRunner,
               max_wait=Amount(1, Time.MINUTES),
               preemption_wait=Amount(1, Time.MINUTES),
               poll_interval=Amount(500, Time.MILLISECONDS),
               clock=time,
               process_logger_mode=None,
               rotate_log_size_mb=None,
               rotate_log_backups=None):
    self._artifact_dir = artifact_dir or safe_mkdtemp()
    self._checkpoint_root = checkpoint_root
    self._preserve_env = preserve_env
    self._clock = clock
    self._max_wait = max_wait
    self._pex_location = pex_location
    self._poll_interval = poll_interval
    self._preemption_wait = preemption_wait
    self._task_runner_class = task_runner_class
    self._process_logger_mode = process_logger_mode
    self._rotate_log_size_mb = rotate_log_size_mb
    self._rotate_log_backups = rotate_log_backups

  def _get_role(self, assigned_task):
    return None if assigned_task.task.container.docker else assigned_task.task.job.role

  def from_assigned_task(self, assigned_task, sandbox):
    task_id = assigned_task.taskId
    role = self._get_role(assigned_task)
    try:
      mesos_task = mesos_task_instance_from_assigned_task(assigned_task)
    except ValueError as e:
      raise TaskError('Could not deserialize Thermos task from AssignedTask: %s' % e)
    mesos_ports = resolve_ports(mesos_task, assigned_task.assignedPorts)

    class ProvidedThermosTaskRunner(self._task_runner_class):
      MAX_WAIT = self._max_wait
      POLL_INTERVAL = self._poll_interval
      THERMOS_PREEMPTION_WAIT = self._preemption_wait

    runner = ProvidedThermosTaskRunner(
        self._pex_location,
        task_id,
        mesos_task.task(),
        role,
        mesos_ports,
        sandbox,
        self._checkpoint_root,
        artifact_dir=self._artifact_dir,
        clock=self._clock,
        hostname=assigned_task.slaveHost,
        process_logger_mode=self._process_logger_mode,
        rotate_log_size_mb=self._rotate_log_size_mb,
        rotate_log_backups=self._rotate_log_backups,
        preserve_env=self._preserve_env)

    return HttpLifecycleManager.wrap(runner, mesos_task, mesos_ports)


class UserOverrideThermosTaskRunnerProvider(DefaultThermosTaskRunnerProvider):
  def set_role(self, role):
    self._role = role

  def _get_role(self, assigned_task):
    return self._role
