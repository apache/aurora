import os
import errno
import time
import json
import urllib
import threading

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader

from twitter.thermos.observer.detector import TaskDetector
from twitter.thermos.observer.muxer    import TaskMuxer
from twitter.thermos.observer.measure  import TaskMeasurer

from twitter.thermos.base import TaskPath
from twitter.thermos.base import Helper
from twitter.thermos.base.ckpt import AlaCarteRunnerState

from gen.twitter.thermos.ttypes import *
from gen.twitter.tcl.ttypes import ThermosTask

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
      active_tasks   = self._detector.get_active_uids()
      finished_tasks = self._detector.get_finished_uids()

      self._muxer.lock()
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
          self._muxer.pop(finished)      # remove from checkpoint monitor
        self._finishes.add(finished)
      self._muxer.unlock()

  def _read_task(self, uid):
    """
      Given a task id, read it.  Memoize already-read tasks.
    """
    task = self._tasks.get(uid, None)
    if task: return task

    uid_map = {
      'active_task_path': self._actives,
      'finished_task_path': self._finishes
    }

    for path_type, uidset in uid_map.iteritems():
      if uid in uidset:
        path = self._pathspec.given(task_id = uid).getpath(path_type)
        try:
          if os.path.exists(path):
            with open(path, "r") as fp:
              rr = ThriftRecordReader(fp, ThermosTask)
              task = rr.read()
              if task:
                self._tasks[uid] = task
                return task
        except Exception as e:
          log.error('Error reading ThermosTask from %s in observer: %s' % (
            path, e))

    return None

  def uid_count(self):
    """
      Return the list of active and finished uids.
    """
    return dict(
      active = len(self._actives),
      finished = len(self._finishes)
    )

  def uids(self, type=None, offset=None, num=None):
    """
      Return the list of uids in a browser-friendly format.

      type = (all|active|finished|None) [default: all]
      offset = offset into the list of uids [default: 0]
      num = number of results to return [default: return rest]
    """
    uidlist = []
    if type is None or type == 'all':
      uidlist += self._actives
      uidlist += self._finishes
    elif type == 'active':
      uidlist += self._actives
    elif type == 'finished':
      uidlist += self._finishes
    uidlist.sort()

    offset = offset if offset is not None else 0
    if offset < 0:
      if len(uidlist) > abs(offset):
        offset = offset % len(uidlist)
      else:
        offset = 0
    if num:
      num += offset
    return dict(
      uids = uidlist[offset:num]
    )

  def _state(self, uid):
    """
      Return the current runner state of a given task id
    """
    if uid in self._actives:
      # TODO(wickman)  Protect this call
      return self._muxer.get_state(uid)
    elif uid in self._states:
      # memoized finished state
      return self._states[uid]
    else:
      # unread finished state, let's read and memoize.
      path = self._pathspec.given(task_id = uid).getpath('runner_checkpoint')
      self._states[uid] = AlaCarteRunnerState(path).state()
      return self._states[uid]
    log.error(TaskObserver.UnexpectedError("Could not find uid: %s" % uid))
    return None

  def _job_stub(self, uid):
    """
      Return the job header of the given task id.
    """
    task = self._read_task(uid)
    if task is None: return {}
    return dict(
      name = task.job.name,
      role = task.job.role,
      user = task.job.user,
      datacenter = task.job.datacenter,
      cluster = task.job.cluster
    )

  def _task_processes(self, uid):
    """
      Return the processes of a task given its uid.

      Returns a map from state to processes in that state, where possible
      states are: waiting, running, success, failed.
    """
    if uid not in self._actives and uid not in self._finishes:
      return {}
    state = self._state(uid)
    if state is None:
      return {}

    waiting, running, success, failed = [], [], [], []
    for process in state.processes:
      runs = state.processes[process].runs
      # No runs ==> nothing started.
      if len(runs) == 0:
        waiting.append(process)
      else:
        if state.processes[process].state == TaskState.ACTIVE:
          if runs[-1].run_state == ProcessRunState.WAITING:
            waiting.append(process)
          else:
            running.append(process)
        elif state.processes[process].state == TaskState.SUCCESS:
          success.append(process)
        elif state.processes[process].state == TaskState.FAILED:
          failed.append(process)
        else:
          # TODO(wickman)  Consider log.error instead of raising.
          raise TaskObserver.UnexpectedState(
            "Unexpected TaskState: %s" % state.processes[process].state)

    return dict(
      waiting = waiting,
      running = running,
      success = success,
      failed = failed
    )


  def _task(self, uid):
    """
      Return composite information about a particular task uid, given the below
      schema.

      X denotes currently unimplemented fields.

      {
         uid: string,
         job: { see _job_stub() }
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

    # Unknown uid.
    if uid not in self._actives and uid not in self._finishes:
      return {}

    task = self._read_task(uid)
    if task is None:
      # TODO(wickman)  Can this happen?
      log.error('Could not find task: %s' % uid)
      return {}

    state = self._state(uid)
    if state is None:
      # TODO(wickman)  Can this happen?
      return {}

    return dict(
       uid = uid,
       job = self._job_stub(uid),
       name = task.name,
       replica = task.replica_id,
       state = TaskState._VALUES_TO_NAMES[state.state],
       # ports
       resource_consumption = dict(
         cpu = self._measurer.current_cpu_by_uid(uid),
         ram = 0,   # TODO(wickman)
         disk = 0   # TODO(wickman)
       ),
       # timeline
       processes = self._task_processes(uid)
    )

  def task(self, uids):
    """
      Return a map from uid => task, given the task schema from _task.
    """
    return dict((uid, self._task(uid)) for uid in uids)

  def _get_process_resource_reservation(self, uid, process_name):
    task = self._read_task(uid)
    if task is None: return {}
    process = Helper.process_from_task(task, process_name)
    if process is None: return {}
    return dict(
      cpu = process.footprint.cpu,
      ram = process.footprint.ram,
      disk = process.footprint.disk
    )

  def _get_process_resource_consumption(self, uid, process_name):
    return dict(
      cpu = self._measurer.current_cpu_by_process(uid, process_name),
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

  def process(self, uid, process, run = None):
    """
      Returns a process run, where the schema is given below:

      {
        process_name: string
        process_run: int
        reserved: { cpu: float, ram: int bytes, disk: int bytes }
        used: { cpu: float, ram: int bytes, disk: int bytes }
        start_time: (time since epoch in millis (utc))
        finish_time: (time since epoch in millis (utc))
        state: string [WAITING, FORKED, RUNNING, FINISHED, KILLED, FAILED, LOST]
      }

      If run is None, return the latest run.
    """
    state = self._state(uid)
    if state is None:
      return {}
    if process not in state.processes: return {}
    history = state.processes[process]

    run = int(run) if run is not None else -1
    tup = self._get_process_tuple(history, run)
    if not tup:
      return {}

    tup.update(reserved = self._get_process_resource_reservation(uid, process))
    if tup.get('state') == 'RUNNING':
      tup.update(used = self._get_process_resource_consumption(uid, process))
    return tup

  def _processes(self, uid):
    """
      Return
        {
          waiting:  [process1, ..., processN]
          running:  [process1, ..., processN]
          success:  [process1, ..., processN]
          failed:   [process1, ..., processN]
        }

      where processK is the latest run of processK and in the schema as
      defined by process().
    """

    if uid not in self._actives and uid not in self._finishes:
      return {}
    state = self._state(uid)
    if state is None:
      return {}

    processes = self._task_processes(uid)
    d = dict(task_uid = uid)
    for process_type in processes:
      d.update(process_type = [self.process(uid, process_name)
        for process_name in processes[process_type]])
    return d

  def processes(self, uids):
    """
      Given a list of uids, returns a map of uid => processes, where processes
      is defined by the schema in _processes.
    """
    return dict((uid, self._processes(uid)) for uid in uids)

  def get_run_number(self, runner_state, process, run = None):
    if runner_state is not None:
      run = run if run is not None else -1
      if run < len(runner_state.processes[process].runs):
        if len(runner_state.processes[process].runs) > 0:
          return run % len(runner_state.processes[process].runs)

  def logs(self, uid, process, run = None):
    """
      Given a uid and a process and (optional) run number, return a dict:
      {
        stderr: [dir, filename]
        stdout: [dir, filename]
      }

      If the run number is unspecified, uses the latest run.

      TODO(wickman)  Just return the filenames directly?
    """
    runner_state = self._state(uid)
    if runner_state is None:
      return {}
    run = self.get_run_number(runner_state, process, run)
    if run is None:
      return {}
    log_path = self._pathspec.given(task_id = uid, process = process, run = run).getpath('process_logdir')
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

  def file_path(self, uid, path):
    """
      Given a uid and a path within that uid's sandbox, verify:
        (1) it's actually in the sandbox and not outside
        (2) it's a valid, existing _file_ within that path
      Returns chroot and the pathname relative to that chroot.
    """
    runner_state = self._state(uid)
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

  def files(self, uid, path = None):
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
    runner_state = self._state(uid)
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
      uid = uid,
      chroot = chroot,
      path = path,
      dirs = dirs,
      files = files
    )
