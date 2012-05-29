from contextlib import closing
import errno
import os
import signal
import time

from twitter.common import log
from twitter.common.dirutil import safe_mkdir, lock_file
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordWriter
from twitter.thermos.base.ckpt import CheckpointDispatcher
from twitter.thermos.base.path import TaskPath

from gen.twitter.thermos.ttypes import (
  ProcessState,
  ProcessStatus,
  RunnerCkpt,
  TaskState,
  TaskStatus)


class TaskKiller(object):
  """
    Task killing interface.
  """
  def __init__(self, task_id, checkpoint_root):
    self._task_id = task_id
    self._checkpoint_root = checkpoint_root

  def kill(self, force=True):
    TaskRunnerHelper.kill(self._task_id, self._checkpoint_root, force=force,
                          terminal_status=TaskState.KILLED)

  def lose(self, force=True):
    TaskRunnerHelper.kill(self._task_id, self._checkpoint_root, force=force,
                          terminal_status=TaskState.LOST)



class TaskRunnerHelper(object):
  """
    TaskRunner helper methods that can be operated directly upon checkpoint
    state.  These operations do not require knowledge of the underlying
    task.
  """
  class PermissionError(Exception): pass

  # Maximum drift between when the system says a task was forked and when we checkpointed
  # its fork_time (used as a heuristic to determine a forked task is really ours instead of
  # a task with coincidentally the same PID but just wrapped around.)
  MAX_START_TIME_DRIFT = Amount(10, Time.SECONDS)

  @staticmethod
  def get_actual_user():
    import getpass, pwd
    try:
      pwd_entry = pwd.getpwuid(os.getuid())
    except KeyError:
      return getpass.getuser()
    return pwd_entry[0]

  @staticmethod
  def process_from_name(task, process_name):
    if task.has_processes():
      for process in task.processes():
        if process.name().get() == process_name:
          return process
    return None

  @staticmethod
  def safe_kill_pid(pid):
    try:
      os.kill(pid, signal.SIGKILL)
    except Exception as e:
      log.error('    (error: %s)' % e)

  @classmethod
  def this_is_really_our_pid(cls, process_handle, current_user, start_time, clock=time):
    """
      A heuristic to make sure that this is likely the pid that we own/forked.  Necessary
      because of pid-space wrapping.  We don't want to go and kill processes we don't own,
      especially if the killer is running as root.
    """
    if process_handle.user() != current_user:
      log.info("    Expected pid %s to be ours but the pid user is %s and we're %s" % (
        process_handle.pid(), process_handle.user(), current_user))
      return False
    estimated_start_time = clock.time() - process_handle.wall_time()
    log.info("    Time drift from the start of %s is %s, real: %s, pid wall: %s, estimated: %s" % (
      process_handle.pid(), abs(start_time - estimated_start_time), start_time,
      process_handle.wall_time(), estimated_start_time))
    return abs(start_time - estimated_start_time) < cls.MAX_START_TIME_DRIFT.as_(Time.SECONDS)

  @classmethod
  def killtree(cls, state, clock=time):
    """
      Kill the process tree associated with the provided task state.

      Returns the tuple of (active, lost) ProcessState objects should the caller
      want to update the state object.
    """
    log.info('Killing task id: %s' % state.header.task_id)
    ps = ProcessProviderFactory.get()
    ps.collect_all()

    coordinator_pids = []
    process_pids = []
    lost_processes, active_processes = [], []

    current_user = state.header.user
    log.info('    => Current user: %s' % current_user)
    log.debug('    => All PIDs on system: %s' % ' '.join(map(str, sorted(ps.pids()))))
    for process_history in state.processes.values():
      # collect coordinator_pids for runners in >=FORKED, <=RUNNING state
      last_run = process_history[-1]
      if last_run.state in (ProcessState.FORKED, ProcessState.RUNNING):
        log.info('  Inspecting %s coordinator [pid: %s]' % (last_run.process,
            last_run.coordinator_pid))
        if last_run.coordinator_pid in ps.pids() and cls.this_is_really_our_pid(
            ps.get_handle(last_run.coordinator_pid), current_user, last_run.fork_time):
          coordinator_pids.append(last_run.coordinator_pid)
        else:
          log.info('    (coordinator appears to have completed)')
      if last_run.state == ProcessState.RUNNING:
        log.info('  Inspecting %s [pid: %s]' % (last_run.process, last_run.pid))
        if last_run.pid in ps.pids() and cls.this_is_really_our_pid(
            ps.get_handle(last_run.pid), current_user, last_run.start_time, clock=clock):
          log.info('    => pid is active.')
          active_processes.append(last_run)
          process_pids.append(last_run.pid)
          subtree = ps.children_of(last_run.pid, all=True)
          if subtree:
            log.info('      => has children: %s' % ' '.join(map(str, subtree)))
            process_pids.extend(subtree)
        else:
          lost_processes.append(last_run)

    pid_types = { 'Coordinator': coordinator_pids, 'Child': process_pids }
    if any(pid_types.values()):
      log.info('Issuing kills.')
    for pid_type, pid_set in pid_types.items():
      for pid in pid_set:
        log.info('  %s: %s' % (pid_type, pid))
        cls.safe_kill_pid(pid)

    return active_processes, lost_processes

  @classmethod
  def kill_runner(cls, state):
    assert state, 'Could not read state!'
    assert state.statuses
    pid = state.statuses[-1].runner_pid
    assert pid != os.getpid(), 'Unwilling to commit seppuku.'
    try:
      os.kill(pid, signal.SIGKILL)
      return True
    except OSError as e:
      if e.errno == errno.EPERM:
        # Permission denied
        return False
      elif e.errno == errno.ESRCH:
        # pid no longer exists
        return True
      raise

  @classmethod
  def open_checkpoint(cls, filename, force=False, state=None):
    """
      Acquire a locked checkpoint stream.
    """
    safe_mkdir(os.path.dirname(filename))
    fp = lock_file(filename, "a+")
    if fp in (None, False):
      if force:
        log.info('Found existing runner, forcing leadership forfeit.')
        state = state or CheckpointDispatcher.from_file(filename)
        if cls.kill_runner(state):
          log.info('Successfully killed leader.')
          # TODO(wickman)  Blocking may not be the best idea here.  Perhaps block up to
          # a maximum timeout.  But blocking is necessary because os.kill does not immediately
          # release the lock if we're in force mode.
          fp = lock_file(filename, "a+", blocking=True)
      else:
        log.error('Found existing runner, cannot take control.')
    if fp in (None, False):
      raise cls.PermissionError('Could not open locked checkpoint: %s, lock_file = %s' %
        (filename, fp))
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    return ckpt

  @classmethod
  def finalize_task(cls, spec):
    active_task = spec.given(state='active').getpath('task_path')
    finished_task = spec.given(state='finished').getpath('task_path')
    is_active, is_finished = map(os.path.exists, [active_task, finished_task])
    assert is_active and not is_finished
    safe_mkdir(os.path.dirname(finished_task))
    os.rename(active_task, finished_task)
    os.utime(finished_task, None)

  @classmethod
  def kill(cls, task_id, checkpoint_root, force=False,
           terminal_status=TaskState.KILLED, clock=time):
    """
      An implementation of Task killing that doesn't require a fully
      hydrated TaskRunner object.  Terminal status must be either
      KILLED or LOST state.
    """
    assert terminal_status in (TaskState.KILLED, TaskState.LOST)
    pathspec = TaskPath(root=checkpoint_root, task_id=task_id)
    checkpoint = pathspec.getpath('runner_checkpoint')
    state = CheckpointDispatcher.from_file(checkpoint)
    ckpt = cls.open_checkpoint(checkpoint, force=force, state=state)
    if state is None or state.header is None or state.statuses is None:
      log.error('Cannot update states in uninitialized TaskState!')
      return

    def write_task_status(status):
      update = TaskStatus(state=status, timestamp_ms=int(clock.time() * 1000),
                          runner_pid=os.getpid(), runner_uid=os.getuid())
      ckpt.write(RunnerCkpt(task_status=update))

    if state.statuses[-1].state is not TaskState.ACTIVE:
      log.info('Task is already in terminal state!  Finalizing.')
      cls.finalize_task(pathspec)
      return

    with closing(ckpt):
      active_processes, lost_processes = cls.killtree(state)
      if active_processes + lost_processes:
        write_task_status(TaskState.ACTIVE)
        for process_status in active_processes:
          status = ProcessStatus(process=process_status.process, state=ProcessState.KILLED,
              seq=process_status.seq + 1, return_code=-1, stop_time=clock.time())
          ckpt.write(RunnerCkpt(process_status=status))
        for process_status in lost_processes:
          status = ProcessStatus(process=process_status.process, state=ProcessState.LOST,
              seq=process_status.seq + 1)
          ckpt.write(RunnerCkpt(process_status=status))
        write_task_status(terminal_status)
      else:
        log.info('Nothing to kill.')

    cls.finalize_task(pathspec)
