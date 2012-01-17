__author__ = 'wickman@twitter.com (brian wickman)'

import copy
from collections import defaultdict

class Planner(object):
  """
     Given a scheduler, determine runnable processes based upon what is running
     and what has finished.
  """

  class InvalidSchedule(Exception): pass

  @staticmethod
  def extract_constraints(task):
    """
      Construct a set of processes and the process dependencies from a Thermos Task.
    """
    processes = set(process.name().get() for process in task.processes())
    dependencies = defaultdict(set)
    if task.has_constraints():
      for constraint in task.constraints():
        for pair in constraint.ordered():
          if pair.first().get() not in processes:
            raise Planner.InvalidSchedule("Unknown process in dependency: %s" % pair.first())
          if pair.second().get() not in processes:
            raise Planner.InvalidSchedule("Unknown process in dependency: %s" % pair.second())
          dependencies[pair.second().get()].add(pair.first().get())
    return (processes, dependencies)

  @staticmethod
  def runnable(processes, dependencies):
    return set(process for process in processes if not dependencies[process])

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
      runnables = Planner.runnable(processes, dependencies)
      if runnables:
        scheduling = True
        processes -= runnables
      dependencies = Planner.filter_dependencies(dependencies, given=runnables)
    return len(processes) == 0

  @staticmethod
  def from_task(task):
    """
      Construct a planner from a Thermos Task.
    """
    processes, dependencies = Planner.extract_constraints(task)
    return Planner(processes, dependencies)

  def __init__(self, processes, dependencies):
    self._processes = processes
    self._dependencies = dependencies
    if not Planner.satisfiable(self._processes, self._dependencies):
      raise Planner.InvalidSchedule("Cycles detected in the task schedule!")
    self._running = set()
    self._finished = set()

  def get_runnable(self):
    return Planner.runnable(self._processes - self._running - self._finished,
      Planner.filter_dependencies(self._dependencies, given=self._finished))

  def get_running(self):
    return list(self._running)

  def get_finished(self):
    return list(self._finished)

  def forget(self, process):
    self._finished.discard(process)
    self._running.discard(process)

  def set_running(self, process):
    self._finished.discard(process)
    self._running.add(process)

  def set_finished(self, process):
    self._running.discard(process)
    self._finished.add(process)

  def is_finished(self, process):
    return process in self._finished

  def is_running(self, process):
    return process in self._running

  def is_complete(self):
    return self._finished == self._processes
