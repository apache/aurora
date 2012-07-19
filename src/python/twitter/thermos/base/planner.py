"""

Planners to schedule processes within Tasks.

TaskPlanner:
  a daemon process can depend upon a regular process
  a regular process can not depend upon a daemon process
"""

from collections import defaultdict, namedtuple
import copy
from functools import partial
import sys
import time

class Planner(object):
  """
    Given a set of process names and a graph of dependencies between them, determine
    what can run predicated upon process completions.
  """
  class InvalidSchedule(Exception): pass

  @staticmethod
  def filter_runnable(processes, dependencies):
    return set(process for process in processes if not dependencies.get(process))

  @staticmethod
  def filter_dependencies(dependencies, given=frozenset()):
    """
      Provided a map of process => list of process :dependencies, and a set of satisfied
      prior processes in :given, return the new map of dependencies with priors removed.
    """
    dependencies = copy.deepcopy(dependencies)
    for process_set in dependencies.values():
      process_set -= given
    return dependencies

  @staticmethod
  def satisfiable(processes, dependencies):
    """
      Given a set of processes and a dependency map, determine if this is a consistent
      schedule without cycles.
    """
    processes = copy.copy(processes)
    dependencies = copy.deepcopy(dependencies)

    scheduling = True
    while scheduling:
      scheduling = False
      runnables = Planner.filter_runnable(processes, dependencies)
      if runnables:
        scheduling = True
        processes -= runnables
      dependencies = Planner.filter_dependencies(dependencies, given=runnables)
    return len(processes) == 0

  def __init__(self, processes, dependencies):
    self._processes = set(processes)
    self._dependencies = dict((process, set(dependencies.get(process, [])))
        for process in self._processes)
    if not Planner.satisfiable(self._processes, self._dependencies):
      raise Planner.InvalidSchedule("Cycles detected in the task schedule!")
    self._running = set()
    self._finished = set()
    self._failed = set()

  @property
  def runnable(self):
    return Planner.filter_runnable(self._processes - self._running - self._finished - self._failed,
      Planner.filter_dependencies(self._dependencies, given=self._finished))

  @property
  def processes(self):
    return set(self._processes)

  @property
  def running(self):
    return set(self._running)

  @property
  def finished(self):
    return set(self._finished)

  @property
  def failed(self):
    return set(self._failed)

  def reset(self, process):
    assert process in self._running
    assert process not in self._finished
    assert process not in self._failed
    self._running.discard(process)

  def set_running(self, process):
    assert process not in self._failed
    assert process not in self._finished
    assert process in self._running or process in self.runnable
    self._running.add(process)

  def set_finished(self, process):
    assert process in self._running
    assert process not in self._failed
    self._running.discard(process)
    self._finished.add(process)

  def set_failed(self, process):
    assert process in self._running
    assert process not in self._finished
    self._running.discard(process)
    self._failed.add(process)

  def is_complete(self):
    return self._finished == (self._processes - self._failed)


TaskAttributes = namedtuple('TaskAttributes', 'min_duration is_daemon max_failures is_ephemeral')

class TaskPlanner(object):
  """
    A planner for processes part of a Thermos task, taking into account ephemeral and daemon
    bits, in addition to duration restrictions [and failure limits?].

                               is_daemon
         .----------------------------------------------------.
         |                                                    |
         |    clock gate      .----------------------.        |
         |  .---------------> | runnable && !waiting |        |
         v  |                 `----------------------'        |
       .----------.                       |                   |
       | runnable |                       | set_running       |
       `----------'                       v                   |
            ^        forget          .---------.              |  !is_daemon  .----------.
            `------------------------| running |--------------+------------> | finished |
            ^                        `---------' add_success                 `----------'
            |                             |
            |     under failure limit     | add_failure
            `-----------------------------+
                                          | past failure limit
                                          v
                                      .--------.
                                      | failed |
                                      `--------'
  """
  InvalidSchedule = Planner.InvalidSchedule
  INFINITY = sys.float_info.max

  @staticmethod
  def extract_dependencies(task, process_filter=None):
    """
      Construct a set of processes and the process dependencies from a Thermos Task.
    """
    process_map = dict((process.name().get(), process)
                        for process in filter(process_filter, task.processes()))
    processes = set(process_map)
    dependencies = defaultdict(set)
    if task.has_constraints():
      for constraint in task.constraints():
        # handle process orders
        process_names = constraint.order().get()
        process_name_set = set(process_names)
        # either all process_names must be in processes or none should be
        if process_name_set.issubset(processes) == process_name_set.isdisjoint(processes):
          raise TaskPlanner.InvalidSchedule('Invalid process dependencies!')
        if not process_name_set.issubset(processes):
          continue
        for k in range(1, len(process_names)):
          pnk, pnk1 = process_names[k], process_names[k-1]
          if pnk1 not in processes:
            raise TaskPlanner.InvalidSchedule("Unknown process in dependency: %s" % pnk1)
          if pnk not in processes:
            raise TaskPlanner.InvalidSchedule("Unknown process in dependency: %s" % pnk)
          if process_map[pnk1].daemon().get():
            raise TaskPlanner.InvalidSchedule("Process %s may not depend upon daemon process %s"
                % (pnk, pnk1))
          dependencies[pnk].add(pnk1)
    return (processes, dependencies)

  def __init__(self, task, clock=time, process_filter=None):
    self._filter = process_filter
    assert self._filter is None or callable(self._filter), (
        'TaskPlanner must be given callable process filter.')
    self._planner = Planner(*self.extract_dependencies(task, self._filter))
    self._clock = clock
    self._last_terminal = {} # process => timestamp of last terminal state
    self._failures = defaultdict(int)
    self._attributes = {}
    self._ephemerals = set(process.name().get() for process in task.processes()
        if (self._filter is None or self._filter(process)) and process.ephemeral().get())

    for process in filter(self._filter, task.processes()):
      self._attributes[process.name().get()] = TaskAttributes(
        is_daemon=bool(process.daemon().get()),
        is_ephemeral=bool(process.ephemeral().get()),
        max_failures=process.max_failures().get(),
        min_duration=process.min_duration().get())

  def get_wait(self, process, timestamp=None):
    now = timestamp if timestamp is not None else self._clock.time()
    if process not in self._last_terminal:
      return 0
    return self._attributes[process].min_duration - (now - self._last_terminal[process])

  def is_ready(self, process, timestamp=None):
    return self.get_wait(process, timestamp) <= 0

  def is_waiting(self, process, timestamp=None):
    return not self.is_ready(process, timestamp)

  @property
  def runnable(self):
    """A list of processes that are runnable w/o duration restrictions."""
    return self.runnable_at(self._clock.time())

  @property
  def waiting(self):
    """A list of processes that are runnable w/o duration restrictions."""
    return self.waiting_at(self._clock.time())

  def runnable_at(self, timestamp):
    return set(filter(partial(self.is_ready, timestamp=timestamp), self._planner.runnable))

  def waiting_at(self, timestamp):
    return set(filter(partial(self.is_waiting, timestamp=timestamp), self._planner.runnable))

  def min_wait(self, timestamp=None):
    """Return the current wait time for the next process to become runnable, 0 if something is ready
       immediately, or sys.float.max if there are no waiters."""
    if self.runnable_at(timestamp if timestamp is not None else self._clock.time()):
      return 0
    waits = [self.get_wait(waiter, timestamp) for waiter in self.waiting_at(timestamp)]
    return min(waits) if waits else self.INFINITY

  def set_running(self, process):
    self._planner.set_running(process)

  def add_failure(self, process, timestamp=None):
    timestamp = timestamp if timestamp is not None else self._clock.time()
    self._last_terminal[process] = timestamp
    self._failures[process] += 1
    if self._failures[process] >= self._attributes[process].max_failures:
      self._planner.set_failed(process)
    else:
      self._planner.reset(process)

  def add_success(self, process, timestamp=None):
    timestamp = timestamp if timestamp is not None else self._clock.time()
    self._last_terminal[process] = timestamp
    if not self._attributes[process].is_daemon:
      self._planner.set_finished(process)
    else:
      self._planner.reset(process)

  def set_failed(self, process):
    """Force a process to be in failed state.  E.g. kill -9 and you want it pinned failed."""
    self._planner.set_failed(process)

  def lost(self, process):
    """Mark a process as lost.  This sets its runnable state back to the previous runnable
       state and does not increment its failure count."""
    self._planner.reset(process)

  def is_complete(self):
    terminals = self._planner.finished.union(self._planner.failed).union(self._ephemerals)
    return self._planner.processes == terminals

  # TODO(wickman) Should we consider subclassing again?
  @property
  def failed(self):
    return self._planner.failed

  @property
  def running(self):
    return self._planner.running

  @property
  def finished(self):
    return self._planner.finished
