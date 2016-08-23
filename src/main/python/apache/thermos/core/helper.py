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
import os
import signal
import time
from contextlib import closing

import psutil
from twitter.common import log
from twitter.common.dirutil import lock_file, safe_mkdir
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordWriter

from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.common.path import TaskPath

from gen.apache.thermos.ttypes import ProcessState, ProcessStatus, RunnerCkpt, TaskState, TaskStatus


class TaskRunnerHelper(object):
  """
    TaskRunner helper methods that can be operated directly upon checkpoint
    state.  These operations do not require knowledge of the underlying
    task.

    TaskRunnerHelper is sort of a mishmash of "checkpoint-only" operations and
    the "Process Platform" stuff that started to get pulled into process.py

    This really needs some hard design thought to see if it can be extracted out
    even further.
  """
  class Error(Exception): pass
  class PermissionError(Error): pass

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

  @classmethod
  def this_is_really_our_pid(cls, process, uid, user, start_time):
    """
      A heuristic to make sure that this is likely the pid that we own/forked.  Necessary
      because of pid-space wrapping.  We don't want to go and kill processes we don't own,
      especially if the killer is running as root.

      process: psutil.Process representing the process to check
      uid: uid expected to own the process (or None if not available)
      user: username expected to own the process
      start_time: time at which it's expected the process has started

      Raises:
        psutil.NoSuchProcess - if the Process supplied no longer exists
    """
    process_create_time = process.create_time()

    if abs(start_time - process_create_time) >= cls.MAX_START_TIME_DRIFT.as_(Time.SECONDS):
      log.info("Expected pid %s start time to be %s but it's %s" % (
          process.pid, start_time, process_create_time))
      return False

    if uid is not None:
      # If the uid was provided, it is gospel, so do not consider user.
      try:
        uids = process.uids()
        if uids is None:
          return False
        process_uid = uids.real
      except psutil.Error:
        return False

      if process_uid == uid:
        return True
      elif uid == 0:
        # If the process was launched as root but is now not root, we should
        # kill this because it could have called `setuid` on itself.
        log.info("pid %s appears to be have launched by root but it's uid is now %s" % (
            process.pid, process_uid))
        return True
      else:
        log.info("Expected pid %s to be ours but the pid uid is %s and we're %s" % (
            process.pid, process_uid, uid))
        return False

    try:
      process_user = process.username()
    except KeyError:
      return False

    if process_user == user:
      # If the uid was not provided, we must use user -- which is possibly flaky if the
      # user gets deleted from the system, so process_user will be None and we must
      # return False.
      log.info("Expected pid %s to be ours but the pid user is %s and we're %s" % (
          process.pid, process_user, user))
      return True

    return False

  @classmethod
  def scan_process(cls, state, process_name):
    """
      Given a RunnerState and a process_name, return the following:
        (coordinator pid, process pid, process tree)
        (int or None, int or None, set)

    """
    process_run = state.processes[process_name][-1]
    user, uid = state.header.user, state.header.uid

    coordinator_pid, pid, tree = None, None, set()

    if uid is None:
      log.debug('Legacy thermos checkpoint stream detected, user = %s' % user)

    if process_run.coordinator_pid:
      try:
        coordinator_process = psutil.Process(process_run.coordinator_pid)
        if cls.this_is_really_our_pid(coordinator_process, uid, user, process_run.fork_time):
          coordinator_pid = process_run.coordinator_pid
      except psutil.NoSuchProcess:
        log.info('  Coordinator %s [pid: %s] completed.' % (process_run.process,
            process_run.coordinator_pid))
      except psutil.Error as err:
        log.warning('  Error gathering information on pid %s: %s' % (process_run.coordinator_pid,
            err))

    if process_run.pid:
      try:
        process = psutil.Process(process_run.pid)
        if cls.this_is_really_our_pid(process, uid, user, process_run.start_time):
          pid = process.pid
      except psutil.NoSuchProcess:
        log.info('  Process %s [pid: %s] completed.' % (process_run.process, process_run.pid))
      except psutil.Error as err:
        log.warning('  Error gathering information on pid %s: %s' % (process_run.pid, err))
      else:
        if pid:
          try:
            tree = set(child.pid for child in process.children(recursive=True))
          except psutil.Error:
            log.warning('  Error gathering information on children of pid %s' % pid)

    return (coordinator_pid, pid, tree)

  @classmethod
  def scan_tree(cls, state):
    """
      Scan the process tree associated with the provided task state.

      Returns a dictionary of process name => (coordinator pid, pid, pid children)
      If the coordinator is no longer active, coordinator pid will be None.  If the
      forked process is no longer active, pid will be None and its children will be
      an empty set.
    """
    return dict((process_name, cls.scan_process(state, process_name))
                for process_name in state.processes)

  @classmethod
  def safe_signal(cls, pid, sig=signal.SIGTERM):
    try:
      os.kill(pid, sig)
    except OSError as e:
      if e.errno not in (errno.ESRCH, errno.EPERM):
        log.error('Unexpected error in os.kill: %s' % e)
    except Exception as e:
      log.error('Unexpected error in os.kill: %s' % e)

  @classmethod
  def terminate_pid(cls, pid):
    cls.safe_signal(pid, signal.SIGTERM)

  @classmethod
  def kill_pid(cls, pid):
    cls.safe_signal(pid, signal.SIGKILL)

  @classmethod
  def kill_group(cls, pgrp):
    cls.safe_signal(-pgrp, signal.SIGKILL)

  @classmethod
  def _get_process_tuple(cls, state, process_name):
    assert process_name in state.processes and len(state.processes[process_name]) > 0
    return cls.scan_process(state, process_name)

  @classmethod
  def _get_coordinator_group(cls, state, process_name):
    assert process_name in state.processes and len(state.processes[process_name]) > 0
    return state.processes[process_name][-1].coordinator_pid

  @classmethod
  def terminate_process(cls, state, process_name):
    log.debug('TaskRunnerHelper.terminate_process(%s)' % process_name)
    _, pid, _ = cls._get_process_tuple(state, process_name)
    if pid:
      log.debug('   => SIGTERM pid %s' % pid)
      cls.terminate_pid(pid)
    return bool(pid)

  @classmethod
  def kill_process(cls, state, process_name):
    log.debug('TaskRunnerHelper.kill_process(%s)' % process_name)
    coordinator_pgid = cls._get_coordinator_group(state, process_name)
    coordinator_pid, pid, tree = cls._get_process_tuple(state, process_name)
    # This is super dangerous.  TODO(wickman)  Add a heuristic that determines
    # that 1) there are processes that currently belong to this process group
    #  and 2) those processes have inherited the coordinator checkpoint filehandle
    # This way we validate that it is in fact the process group we expect.
    if coordinator_pgid:
      log.debug('   => SIGKILL coordinator group %s' % coordinator_pgid)
      cls.kill_group(coordinator_pgid)
    if coordinator_pid:
      log.debug('   => SIGKILL coordinator %s' % coordinator_pid)
      cls.kill_pid(coordinator_pid)
    if pid:
      log.debug('   => SIGKILL pid %s' % pid)
      cls.kill_pid(pid)
    for child in tree:
      log.debug('   => SIGKILL child %s' % child)
      cls.kill_pid(child)
    return bool(coordinator_pid or pid or tree)

  @classmethod
  def kill_runner(cls, state):
    log.debug('TaskRunnerHelper.kill_runner()')
    if not state or not state.statuses:
      raise cls.Error('Could not read state!')
    pid = state.statuses[-1].runner_pid
    if pid == os.getpid():
      raise cls.Error('Unwilling to commit seppuku.')
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
  def kill(cls, task_id, checkpoint_root, force=False,
           terminal_status=TaskState.KILLED, clock=time):
    """
      An implementation of Task killing that doesn't require a fully hydrated TaskRunner object.
      Terminal status must be either KILLED or LOST state.
    """
    if terminal_status not in (TaskState.KILLED, TaskState.LOST):
      raise cls.Error('terminal_status must be KILLED or LOST (got %s)' %
                      TaskState._VALUES_TO_NAMES.get(terminal_status) or terminal_status)
    pathspec = TaskPath(root=checkpoint_root, task_id=task_id)
    checkpoint = pathspec.getpath('runner_checkpoint')
    state = CheckpointDispatcher.from_file(checkpoint)

    if state is None or state.header is None or state.statuses is None:
      if force:
        log.error('Task has uninitialized TaskState - forcibly finalizing')
        cls.finalize_task(pathspec)
        return
      else:
        log.error('Cannot update states in uninitialized TaskState!')
        return

    ckpt = cls.open_checkpoint(checkpoint, force=force, state=state)

    def write_task_state(state):
      update = TaskStatus(state=state, timestamp_ms=int(clock.time() * 1000),
                          runner_pid=os.getpid(), runner_uid=os.getuid())
      ckpt.write(RunnerCkpt(task_status=update))

    def write_process_status(status):
      ckpt.write(RunnerCkpt(process_status=status))

    if cls.is_task_terminal(state.statuses[-1].state):
      log.info('Task is already in terminal state!  Finalizing.')
      cls.finalize_task(pathspec)
      return

    with closing(ckpt):
      write_task_state(TaskState.ACTIVE)
      for process, history in state.processes.items():
        process_status = history[-1]
        if not cls.is_process_terminal(process_status.state):
          if cls.kill_process(state, process):
            write_process_status(ProcessStatus(process=process,
              state=ProcessState.KILLED, seq=process_status.seq + 1, return_code=-9,
              stop_time=clock.time()))
          else:
            if process_status.state is not ProcessState.WAITING:
              write_process_status(ProcessStatus(process=process,
                state=ProcessState.LOST, seq=process_status.seq + 1))
      write_task_state(terminal_status)
    cls.finalize_task(pathspec)

  @classmethod
  def reap_children(cls):
    pids = set()

    while True:
      try:
        pid, status, rusage = os.wait3(os.WNOHANG)
        if pid == 0:
          break
        pids.add(pid)
        log.debug('Detected terminated process: pid=%s, status=%s, rusage=%s' % (
          pid, status, rusage))
      except OSError as e:
        if e.errno != errno.ECHILD:
          log.warning('Unexpected error when calling waitpid: %s' % e)
        break

    return pids

  TERMINAL_PROCESS_STATES = frozenset([
    ProcessState.SUCCESS,
    ProcessState.KILLED,
    ProcessState.FAILED,
    ProcessState.LOST])

  TERMINAL_TASK_STATES = frozenset([
    TaskState.SUCCESS,
    TaskState.FAILED,
    TaskState.KILLED,
    TaskState.LOST])

  @classmethod
  def is_process_terminal(cls, process_status):
    return process_status in cls.TERMINAL_PROCESS_STATES

  @classmethod
  def is_task_terminal(cls, task_status):
    return task_status in cls.TERMINAL_TASK_STATES

  @classmethod
  def initialize_task(cls, spec, task):
    active_task = spec.given(state='active').getpath('task_path')
    finished_task = spec.given(state='finished').getpath('task_path')
    is_active, is_finished = os.path.exists(active_task), os.path.exists(finished_task)
    if is_finished:
      raise cls.Error('Cannot initialize task with "finished" record!')
    if not is_active:
      safe_mkdir(os.path.dirname(active_task))
      with open(active_task, 'w') as fp:
        fp.write(task)

  @classmethod
  def finalize_task(cls, spec):
    active_task = spec.given(state='active').getpath('task_path')
    finished_task = spec.given(state='finished').getpath('task_path')
    is_active, is_finished = os.path.exists(active_task), os.path.exists(finished_task)
    if not is_active:
      raise cls.Error('Cannot finalize task with no "active" record!')
    elif is_finished:
      raise cls.Error('Cannot finalize task with "finished" record!')
    safe_mkdir(os.path.dirname(finished_task))
    os.rename(active_task, finished_task)
    os.utime(finished_task, None)
