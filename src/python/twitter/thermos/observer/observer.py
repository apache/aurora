import os
import errno
import time
import json
import urllib
import threading
from collections import defaultdict

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

from twitter.thermos.observer.detector import TaskDetector
from twitter.thermos.observer.muxer import TaskMuxer
from twitter.thermos.observer.measure import TaskMeasurer

from twitter.thermos.base import TaskPath
from twitter.thermos.base import Helper
from twitter.thermos.base.ckpt import AlaCarteRunnerState


from twitter.thermos.config.schema import (
  Environment,
  ThermosContext)
from twitter.thermos.config.loader import (
  ThermosTaskWrapper,
  ThermosProcessWrapper)

from gen.twitter.thermos.ttypes import *


__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class TaskObserver(threading.Thread):
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
    self._muxer    = TaskMuxer(self._pathspec)
    self._measurer = TaskMeasurer(self._muxer)
    self._measurer.start()
    self._states   = {}
    self._actives  = set()  # set of active task_ids
    self._finishes = set()  # set of finished task_ids
    self._tasks    = {}     # task_id => ThermosTask
    threading.Thread.__init__(self)
    self.daemon = True

  def run(self):
    """
      The internal thread for the observer.  This periodically polls the
      checkpoint root for new tasks, or transitions of tasks from active to
      finished state.
    """
    total_seconds = 0

    while True:
      time.sleep(1)
      total_seconds += 1
      active_tasks   = self._detector.get_active_task_ids()
      finished_tasks = self._detector.get_finished_task_ids()

      for active in active_tasks:
        if active in self._finishes:
          log.error('Found an active (%s) in finished tasks?' % active)
        if active not in self._actives:
          self._actives.add(active)    # add to active list
          self._muxer.add(active)      # add to checkpoint monitor
          self._read_task(active)      # read and memoize ThermosTask object
          log.debug('pid %s -> active' % active)
      for finished in finished_tasks:
        if finished in self._actives:
          log.debug('pid %s active -> finished' % finished)
          self._actives.remove(finished) # remove from actives
          self._muxer.remove(finished)   # remove from checkpoint monitor
        self._finishes.add(finished)

  def _read_task(self, task_id):
    """
      Given a task id, read it.  Memoize already-read tasks.
    """
    task = self._tasks.get(task_id, None)
    if task:
      return task.task()

    task_id_map = {
      'active_task_path': self._actives,
      'finished_task_path': self._finishes
    }

    for path_type, task_idset in task_id_map.iteritems():
      if task_id in task_idset:
        path = self._pathspec.given(task_id = task_id).getpath(path_type)
        if os.path.exists(path):
          task = ThermosTaskWrapper.from_file(path)
          if task is None:
            log.error('Error reading ThermosTask from %s in observer.' % path)
          else:
            self._tasks[task_id] = task
            return task.task()
    return None

  def task_id_count(self):
    """
      Return the list of active and finished task_ids.
    """
    return dict(
      active = len(self._actives),
      finished = len(self._finishes)
    )

  def task_ids(self, type=None, offset=None, num=None):
    """
      Return the list of task_ids in a browser-friendly format.

      type = (all|active|finished|None) [default: all]
      offset = offset into the list of task_ids [default: 0]
      num = number of results to return [default: return rest]
    """
    task_idlist = []
    if type is None or type == 'all':
      task_idlist += self._actives
      task_idlist += self._finishes
    elif type == 'active':
      task_idlist += self._actives
    elif type == 'finished':
      task_idlist += self._finishes
    task_idlist.sort()

    offset = offset if offset is not None else 0
    if offset < 0:
      if len(task_idlist) > abs(offset):
        offset = offset % len(task_idlist)
      else:
        offset = 0
    if num:
      num += offset
    return dict(
      task_ids = task_idlist[offset:num]
    )

  def context(self, task_id):
    state = self._state(task_id)
    if state is None:
      return None
    return ThermosContext(
      ports = state.ports if state.ports is not None else {},
      task_id = state.header.task_id,
      user = state.header.user,
    )

  def state(self, task_id):
    real_state = self._state(task_id)
    if real_state is None or real_state.header is None:
      return {}
    else:
      return dict(
        task_id = real_state.header.task_id,
        launch_time = real_state.header.launch_time,
        sandbox = real_state.header.sandbox,
        hostname = real_state.header.hostname,
        user = real_state.header.user
      )

  def _state(self, task_id):
    """
      Return the current runner state of a given task id
    """
    if task_id in self._actives:
      # TODO(wickman)  Protect this call
      return self._muxer.get_state(task_id)
    elif task_id in self._states:
      # memoized finished state
      return self._states[task_id]
    else:
      # unread finished state, let's read and memoize.
      path = self._pathspec.given(task_id = task_id).getpath('runner_checkpoint')
      self._states[task_id] = AlaCarteRunnerState(path).state()
      return self._states[task_id]
    log.error(TaskObserver.UnexpectedError("Could not find task_id: %s" % task_id))
    return None

  def _task_processes(self, task_id):
    """
      Return the processes of a task given its task_id.

      Returns a map from state to processes in that state, where possible
      states are: waiting, running, success, failed.
    """
    if task_id not in self._actives and task_id not in self._finishes:
      return {}
    state = self._state(task_id)
    if state is None:
      return {}

    waiting, running, success, failed, killed = [], [], [], [], []
    for process in state.processes:
      runs = state.processes[process].runs
      # No runs ==> nothing started.
      if len(runs) == 0:
        waiting.append(process)
      else:
        if state.processes[process].state == TaskRunState.ACTIVE:
          if runs[-1].run_state == ProcessRunState.WAITING:
            waiting.append(process)
          else:
            running.append(process)
        elif state.processes[process].state == TaskRunState.SUCCESS:
          success.append(process)
        elif state.processes[process].state == TaskRunState.FAILED:
          failed.append(process)
        elif state.processes[process].state == TaskRunState.KILLED:
          killed.append(process)
        else:
          # TODO(wickman)  Consider log.error instead of raising.
          raise TaskObserver.UnexpectedState(
            "Unexpected TaskState: %s" % state.processes[process].state)

    return dict(
      waiting = waiting,
      running = running,
      success = success,
      failed = failed,
      killed = killed
    )

  def _task(self, task_id):
    """
      Return composite information about a particular task task_id, given the below
      schema.

      X denotes currently unimplemented fields.

      {
         task_id: string,
         name: string,
         replica: int,
         state: string [ACTIVE, SUCCESS, FAILED]
      X  ports: { name1: 'url', name2: 'url2' }
         resource_consumption: {cpu: , ram: , disk:}
      X  timeline: {
            axes: [ 'time', 'cpu', 'ram', 'disk'],
            data: [ (1299534274324, 0.9, 1212492188, 493932999392),
                    (1299534275327, 0.92, 23432423423, 52353252343), ... ]
         }
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

    state = self._state(task_id)
    if state is None:
      # TODO(wickman)  Can this happen?
      return {}

    context = self.context(task_id)
    task = task % Environment(thermos = context)

    return dict(
       task_id = task_id,
       name = task.name().get(),
       state = TaskState._VALUES_TO_NAMES[state.state],
       user = task.user().get(),
       # ports
       resource_consumption = dict(
         cpu = self._measurer.current_cpu_by_task_id(task_id),
         ram = 0,   # TODO(wickman)
         disk = 0   # TODO(wickman)
       ),
       # timeline
       processes = self._task_processes(task_id)
    )

  def task(self, task_ids):
    """
      Return a map from task_id => task, given the task schema from _task.
    """
    return dict((task_id, self._task(task_id)) for task_id in task_ids)

  def _get_process_resource_consumption(self, task_id, process_name):
    return dict(
      cpu = self._measurer.current_cpu_by_process(task_id, process_name),
      ram = 0, # implement
      disk = 0 # implement
    )

  def _get_process_tuple(self, history, run):
    """
      Return the basic description of a process run if it exists, otherwise
      an empty dictionary.

      {
        process_name: string
        process_run: int
        state: string [WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST]
        (optional) start_time: milliseconds from epoch
        (optional) stop_time: milliseconds from epoch
      }
    """
    if len(history.runs) == 0:
      return {}
    if run >= len(history.runs):
      return {}
    else:
      process_run = history.runs[run]
      run = run % len(history.runs)
      d = dict(
        process_name = process_run.process,
        process_run = run,
        state = ProcessRunState._VALUES_TO_NAMES[process_run.run_state],
      )
      if process_run.start_time:
        d.update(start_time = process_run.start_time * 1000)
      if process_run.stop_time:
        d.update(stop_time = process_run.stop_time * 1000)
      return d

  def process(self, task_id, process, run = None):
    """
      Returns a process run, where the schema is given below:

      {
        process_name: string
        process_run: int
        used: { cpu: float, ram: int bytes, disk: int bytes }
        start_time: (time since epoch in millis (utc))
        stop_time: (time since epoch in millis (utc))
        state: string [WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST]
      }

      If run is None, return the latest run.
    """
    state = self._state(task_id)
    if state is None:
      return {}
    if process not in state.processes: return {}
    history = state.processes[process]

    run = int(run) if run is not None else -1
    tup = self._get_process_tuple(history, run)
    if not tup:
      return {}

    if tup.get('state') == 'RUNNING':
      tup.update(used = self._get_process_resource_consumption(task_id, process))
    return tup

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
    state = self._state(task_id)
    if state is None:
      return {}

    processes = self._task_processes(task_id)
    d = dict()
    for process_type in processes:
      for process_name in processes[process_type]:
        d.update({ process_name: self.process(task_id, process_name) })
    return d

  def processes(self, task_ids):
    """
      Given a list of task_ids, returns a map of task_id => processes, where processes
      is defined by the schema in _processes.
    """
    if not isinstance(task_ids, (list, tuple)):
      return {}
    return dict((task_id, self._processes(task_id)) for task_id in task_ids)

  def get_run_number(self, runner_state, process, run = None):
    if runner_state is not None:
      run = run if run is not None else -1
      if run < len(runner_state.processes[process].runs):
        if len(runner_state.processes[process].runs) > 0:
          return run % len(runner_state.processes[process].runs)

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
    runner_state = self._state(task_id)
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

  def file_path(self, task_id, path):
    """
      Given a task_id and a path within that task_id's sandbox, verify:
        (1) it's actually in the sandbox and not outside
        (2) it's a valid, existing _file_ within that path
      Returns chroot and the pathname relative to that chroot.
    """
    runner_state = self._state(task_id)
    if runner_state is None:
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
    runner_state = self._state(task_id)
    if runner_state is None:
      return {}
    try:
      chroot = runner_state.header.sandbox
    except:
      return {}
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
