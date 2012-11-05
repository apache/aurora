import os
import errno
import time
import json
import urllib
import threading
from collections import defaultdict

from twitter.common import log
from twitter.common.lang import Lockable
from twitter.common.recordio import ThriftRecordReader

from twitter.thermos.monitoring.detector import TaskDetector
from twitter.thermos.monitoring.muxer import TaskMuxer
from twitter.thermos.monitoring.measure import TaskMeasurer

from twitter.thermos.base.path import TaskPath
from twitter.thermos.base.ckpt import CheckpointDispatcher

from pystachio import Environment
from twitter.thermos.config.schema import ThermosContext
from twitter.thermos.config.loader import (
  ThermosTaskWrapper,
  ThermosProcessWrapper)

from gen.twitter.thermos.ttypes import *

__author__ = 'wickman@twitter.com (brian wickman)'


class TaskObserver(threading.Thread, Lockable):
  """
    The task observer monitors the thermos checkpoint root for active/finished
    tasks.  It is used to be the oracle of the state of all thermos tasks on
    a machine.

    It currently returns JSON, but really should just return objects.  We should
    then build an object->json translator.
  """
  class UnexpectedError(Exception): pass
  class UnexpectedState(Exception): pass

  def __init__(self, root):
    self._pathspec = TaskPath(root = root)
    self._detector = TaskDetector(root)
    self._muxer = TaskMuxer(self._pathspec)
    self._measurer = TaskMeasurer(self._muxer)
    self._measurer.start()
    self._states = {}
    self._actives = set()   # set of active task_ids
    self._finishes = set()  # set of finished task_ids
    self._tasks = {}        # task_id => ThermosTask
    self._stat = {}         # task_id => mtime of task file
    threading.Thread.__init__(self)
    Lockable.__init__(self)
    self.daemon = True

  def run(self):
    """
      The internal thread for the observer.  This periodically polls the
      checkpoint root for new tasks, or transitions of tasks from active to
      finished state.
    """
    while True:
      time.sleep(1)

      active_tasks = [task_id for _, task_id in self._detector.get_task_ids(state='active')]
      finished_tasks = [task_id for _, task_id in self._detector.get_task_ids(state='finished')]

      with self.lock:
        for active in active_tasks:
          if active in self._finishes:
            log.error('Found an active (%s) in finished tasks?' % active)
          if active not in self._actives:
            self._actives.add(active)    # add to active list
            self._muxer.add(active)      # add to checkpoint monitor
            self._read_task(active)      # read and memoize ThermosTask object
            self._stat[active] = self._get_stat(active)
            log.debug('pid %s -> active' % active)
        for finished in finished_tasks:
          if finished in self._actives:
            log.debug('pid %s active -> finished' % finished)
            self._actives.remove(finished) # remove from actives
            self._muxer.remove(finished)   # remove from checkpoint monitor
          if finished not in self._stat:
            self._stat[finished] = self._get_stat(finished)
          self._finishes.add(finished)

  def _get_stat(self, task_id):
    ps = self._pathspec.given(task_id=task_id)
    try:
      return os.path.getmtime(ps.given(state='active').getpath('task_path'))
    except OSError as e:
      if e.errno != errno.ENOENT:
        raise
      return os.path.getmtime(ps.given(state='finished').getpath('task_path'))

  @Lockable.sync
  def process_from_name(self, task_id, process_id):
    task = self._read_task(task_id)
    if task is None:
      return None
    for process in task.processes():
      if process.name().get() == process_id:
        return process
    return None

  @Lockable.sync
  def _read_task(self, task_id):
    """
      Given a task id, read it.  Memoize already-read tasks.
    """
    task = self._tasks.get(task_id, None)
    if task:
      return task

    task_id_map = {
      'active': self._actives,
      'finished': self._finishes
    }

    for state_type, task_idset in task_id_map.iteritems():
      if task_id in task_idset:
        path = self._pathspec.given(task_id=task_id, state=state_type).getpath('task_path')
        if os.path.exists(path):
          task = ThermosTaskWrapper.from_file(path)
          if task is None:
            log.error('Error reading ThermosTask from %s in observer.' % path)
          else:
            context = self.context(task_id)
            if not context:
              log.warning('Task not yet available: %s' % task_id)
              return None
            task = task.task() % Environment(thermos = context)
            self._tasks[task_id] = task
            return task
    return None

  @Lockable.sync
  def task_id_count(self):
    """
      Return the list of active and finished task_ids.
    """
    return dict(
      active = len(self._actives),
      finished = len(self._finishes)
    )

  @Lockable.sync
  def task_ids(self, type=None, offset=None, num=None):
    """
      Return the list of task_ids in a browser-friendly format.
      Task ids are sorted by interest:
        - active tasks are sorted by start time
        - finished tasks are sorted by completion time

      type = (all|active|finished|None) [default: all]
      offset = offset into the list of task_ids [default: 0]
      num = number of results to return [default: return 20]

      Returns {
        task_ids: [task_id_1, ..., task_id_N],
        type: query type,
        offset: next offset,
        num: next num
      }
    """
    num = num or 20
    offset = offset or 0
    type = type or 'all'

    if type == 'all':
      tids = list(self._actives) + list(self._finishes)
    elif type == 'active':
      tids = list(self._actives)
    elif type == 'finished':
      tids = list(self._finishes)
    else:
      raise ValueError('Unknown task type %s' % type)

    tids = sorted([(tid, self._stat.get(tid, 0)) for tid in tids], key=lambda pair: pair[1],
        reverse=True)

    end = num
    if offset < 0:
      offset = offset % len(tids) if len(tids) > abs(offset) else 0
    end += offset

    return dict(
        task_ids=[v[0] for v in tids[offset:end]],
        type=type,
        offset=offset,
        num=num)

  def context(self, task_id):
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}
    return ThermosContext(
      ports = state.header.ports if state.header and state.header.ports else {},
      task_id = state.header.task_id,
      user = state.header.user,
    )

  @Lockable.sync
  def state(self, task_id):
    real_state = self.raw_state(task_id)
    if real_state is None or real_state.header is None:
      return {}
    else:
      return dict(
        task_id = real_state.header.task_id,
        launch_time = real_state.header.launch_time_ms/1000.0,
        sandbox = real_state.header.sandbox,
        hostname = real_state.header.hostname,
        user = real_state.header.user
      )

  @Lockable.sync
  def raw_state(self, task_id):
    """
      Return the current runner state (thrift blob: gen.twitter.thermos.ttypes.RunnerState)
      of a given task id
    """
    if task_id in self._actives:
      return self._muxer.get_state(task_id)
    elif task_id in self._states:
      # memoized finished state
      return self._states[task_id]
    else:
      # unread finished state, let's read and memoize.
      path = self._pathspec.given(task_id = task_id).getpath('runner_checkpoint')
      self._states[task_id] = CheckpointDispatcher.from_file(path)
      return self._states[task_id]
    log.error("Could not find task_id: %s" % task_id)
    return None

  @Lockable.sync
  def _task_processes(self, task_id):
    """
      Return the processes of a task given its task_id.

      Returns a map from state to processes in that state, where possible
      states are: waiting, running, success, failed.
    """
    if task_id not in self._actives and task_id not in self._finishes:
      return {}
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}

    waiting, running, success, failed, killed = [], [], [], [], []
    for process, runs in state.processes.items():
      # No runs ==> nothing started.
      if len(runs) == 0:
        waiting.append(process)
      else:
        if runs[-1].state in (None, ProcessState.WAITING, ProcessState.LOST):
          waiting.append(process)
        elif runs[-1].state in (ProcessState.FORKED, ProcessState.RUNNING):
          running.append(process)
        elif runs[-1].state == ProcessState.SUCCESS:
          success.append(process)
        elif runs[-1].state == ProcessState.FAILED:
          failed.append(process)
        elif runs[-1].state == ProcessState.KILLED:
          killed.append(process)
        else:
          # TODO(wickman)  Consider log.error instead of raising.
          raise TaskObserver.UnexpectedState(
            "Unexpected ProcessHistoryState: %s" % state.processes[process].state)

    return dict(
      waiting = waiting,
      running = running,
      success = success,
      failed = failed,
      killed = killed
    )

  @Lockable.sync
  def main(self, type=None, offset=None, num=None):
    task_map = self.task_ids(type, offset, num)
    task_ids = task_map['task_ids']
    tasks = self.task(task_ids)
    if type in ('active', 'finished'):
      task_count = self.task_id_count()[type]
    elif type == 'all':
      task_count = sum(self.task_id_count().values())
    else:
      raise ValueError('Unknown task type %s' % type)
    def task_row(tid):
      task = tasks[tid]
      if task:
        return dict(
            task_id=tid,
            name=task['name'],
            role=task['user'],
            launch_timestamp=task['launch_timestamp'],
            state=task['state'],
            state_timestamp=task['state_timestamp'],
            ports=task['ports'],
            **task['resource_consumption'])
    return dict(
      tasks=filter(None, map(task_row, task_ids)),
      type=task_map['type'],
      offset=task_map['offset'],
      num=task_map['num'],
      task_count=task_count)

  def _sample(self, task_id):
    sample = self._measurer.sample_by_task_id(task_id).to_dict()
    sample['disk'] = 0
    return sample

  @Lockable.sync
  def task_statuses(self, task_id):
    """
      Return the sequence of task states.

      [(task_state [string], timestamp), ...]
    """

    # Unknown task_id.
    if task_id not in self._actives and task_id not in self._finishes:
      return []

    task = self._read_task(task_id)
    if task is None:
      return []

    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return []

    # Get the timestamp of the transition into the current state.
    return [
      (TaskState._VALUES_TO_NAMES.get(state.state, 'UNKNOWN'), state.timestamp_ms / 1000)
      for state in state.statuses]

  @Lockable.sync
  def _task(self, task_id):
    """
      Return composite information about a particular task task_id, given the below
      schema.

      {
         task_id: string,
         name: string,
         user: string,
         launch_timestamp: seconds,
         state: string [ACTIVE, SUCCESS, FAILED]
         ports: { name1: 'url', name2: 'url2' }
         resource_consumption: { cpu:, ram:, disk: }
         processes: { -> names only
            waiting: [],
            running: [],
            success: [],
            failed:  []
         }
      }
    """

    # Unknown task_id.
    if task_id not in self._actives and task_id not in self._finishes:
      return {}

    task = self._read_task(task_id)
    if task is None:
      # TODO(wickman)  Can this happen?
      log.error('Could not find task: %s' % task_id)
      return {}

    state = self.raw_state(task_id)
    if state is None or state.header is None:
      # TODO(wickman)  Can this happen?
      return {}

    # Get the timestamp of the transition into the current state.
    current_state = state.statuses[-1].state
    last_state = state.statuses[0]
    state_timestamp = 0
    for status in state.statuses:
      if status.state == current_state and last_state != current_state:
        state_timestamp = status.timestamp_ms / 1000
      last_state = status.state

    return dict(
       task_id = task_id,
       name = task.name().get(),
       launch_timestamp = state.statuses[0].timestamp_ms / 1000,
       state = TaskState._VALUES_TO_NAMES[state.statuses[-1].state],
       state_timestamp = state_timestamp,
       user = task.user().get(),
       resource_consumption = self._sample(task_id),
       ports = state.header.ports,
       processes = self._task_processes(task_id),
       task_struct = task
    )

  @Lockable.sync
  def task(self, task_ids):
    """
      Return a map from task_id => task, given the task schema from _task.
    """
    return dict((task_id, self._task(task_id)) for task_id in task_ids)

  @Lockable.sync
  def _get_process_resource_consumption(self, task_id, process_name):
    sample = self._measurer.sample_by_process(task_id, process_name).to_dict()
    sample['disk'] = 0
    log.debug('Resource consumption (%s, %s) => %s' % (task_id, process_name, sample))
    return sample

  @Lockable.sync
  def _get_process_tuple(self, history, run):
    """
      Return the basic description of a process run if it exists, otherwise
      an empty dictionary.

      {
        process_name: string
        process_run: int
        state: string [WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST]
        (optional) start_time: seconds from epoch
        (optional) stop_time: seconds from epoch
      }
    """
    if len(history) == 0:
      return {}
    if run >= len(history):
      return {}
    else:
      process_run = history[run]
      run = run % len(history)
      d = dict(
        process_name = process_run.process,
        process_run = run,
        state = ProcessState._VALUES_TO_NAMES[process_run.state],
      )
      if process_run.start_time:
        d.update(start_time = process_run.start_time)
      if process_run.stop_time:
        d.update(stop_time = process_run.stop_time)
      return d

  @Lockable.sync
  def process(self, task_id, process, run = None):
    """
      Returns a process run, where the schema is given below:

      {
        process_name: string
        process_run: int
        used: { cpu: float, ram: int bytes, disk: int bytes }
        start_time: (time since epoch in millis (utc))
        stop_time: (time since epoch in millis (utc))
        state: string [WAITING, FORKED, RUNNING, SUCCESS, KILLED, FAILED, LOST]
      }

      If run is None, return the latest run.
    """
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}
    if process not in state.processes:
      return {}
    history = state.processes[process]
    run = int(run) if run is not None else -1
    tup = self._get_process_tuple(history, run)
    if not tup:
      return {}
    if tup.get('state') == 'RUNNING':
      tup.update(used = self._get_process_resource_consumption(task_id, process))
    return tup

  @Lockable.sync
  def _processes(self, task_id):
    """
      Return
        {
          process1: { ... }
          process2: { ... }
          ...
          processN: { ... }
        }

      where processK is the latest run of processK and in the schema as
      defined by process().
    """

    if task_id not in self._actives and task_id not in self._finishes:
      return {}
    state = self.raw_state(task_id)
    if state is None or state.header is None:
      return {}

    processes = self._task_processes(task_id)
    d = dict()
    for process_type in processes:
      for process_name in processes[process_type]:
        d.update({ process_name: self.process(task_id, process_name) })
    return d

  @Lockable.sync
  def processes(self, task_ids):
    """
      Given a list of task_ids, returns a map of task_id => processes, where processes
      is defined by the schema in _processes.
    """
    if not isinstance(task_ids, (list, tuple)):
      return {}
    return dict((task_id, self._processes(task_id)) for task_id in task_ids)

  @Lockable.sync
  def get_run_number(self, runner_state, process, run = None):
    if runner_state is not None and runner_state.processes is not None:
      run = run if run is not None else -1
      if run < len(runner_state.processes[process]):
        if len(runner_state.processes[process]) > 0:
          return run % len(runner_state.processes[process])

  @Lockable.sync
  def logs(self, task_id, process, run = None):
    """
      Given a task_id and a process and (optional) run number, return a dict:
      {
        stderr: [dir, filename]
        stdout: [dir, filename]
      }

      If the run number is unspecified, uses the latest run.

      TODO(wickman)  Just return the filenames directly?
    """
    runner_state = self.raw_state(task_id)
    if runner_state is None:
      return {}
    run = self.get_run_number(runner_state, process, run)
    if run is None:
      return {}
    log_path = self._pathspec.given(task_id = task_id, process = process, run = run).getpath('process_logdir')
    return dict(
      stdout = [log_path, 'stdout'],
      stderr = [log_path, 'stderr']
    )

  @staticmethod
  def _sanitize_path(base_path, relpath):
    """
      Attempts to sanitize a path through path normalization, also making sure
      that the relative path is contained inside of base_path.
    """
    if relpath is None:
      relpath = "."
    normalized_base = os.path.realpath(base_path)
    normalized = os.path.realpath(os.path.join(base_path, relpath))
    if normalized.startswith(normalized_base):
      return (normalized_base, os.path.relpath(normalized, normalized_base))
    return (None, None)

  @Lockable.sync
  def file_path(self, task_id, path):
    """
      Given a task_id and a path within that task_id's sandbox, verify:
        (1) it's actually in the sandbox and not outside
        (2) it's a valid, existing _file_ within that path
      Returns chroot and the pathname relative to that chroot.
    """
    runner_state = self.raw_state(task_id)
    if runner_state is None or runner_state.header is None:
      return None, None
    try:
      chroot = runner_state.header.sandbox
    except:
      return None, None
    chroot, path = self._sanitize_path(chroot, path)
    if chroot and path:
      if os.path.isfile(os.path.join(chroot, path)):
        return chroot, path
    return None, None

  @Lockable.sync
  def files(self, task_id, path = None):
    """
      Returns dictionary
      {
        chroot: absolute directory on machine
        path: sanitized relative path w.r.t. chroot
        dirs: list of directories
        files: list of files
      }
    """
    path = path if path is not None else '.'
    runner_state = self.raw_state(task_id)
    if runner_state is None:
      return {}
    try:
      chroot = runner_state.header.sandbox
    except:
      return {}
    if chroot is None:  # chroot-less job
      return dict(task_id=task_id, chroot=None, path=None, dirs=None, files=None)
    chroot, path = self._sanitize_path(chroot, path)
    if chroot is None or path is None:
      return {}
    if os.path.isfile(os.path.join(chroot, path)):
      return {}
    names = os.listdir(os.path.join(chroot, path))
    dirs, files = [], []
    for name in names:
      if os.path.isdir(os.path.join(chroot, path, name)):
        dirs.append(name)
      else:
        files.append(name)
    return dict(
      task_id = task_id,
      chroot = chroot,
      path = path,
      dirs = dirs,
      files = files
    )
