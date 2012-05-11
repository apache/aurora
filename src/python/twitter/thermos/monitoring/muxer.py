import threading
from twitter.common.lang import Lockable
from twitter.thermos.monitoring.monitor import TaskMonitor

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class TaskMuxer(Lockable):
  """
    Compose multiple TaskMonitors together into a TaskMuxer.
  """

  class UnknownTaskError(Exception): pass
  class TaskExistsError(Exception): pass

  def __init__(self, pathspec):
    self._pathspec = pathspec
    self._tasks = {}
    Lockable.__init__(self)

  @Lockable.sync
  def add(self, task_id):
    """
      Add a task_id to the monitored collection.

      If the task_id is already being monitored, raises TaskExistsError.
    """
    if task_id in self._tasks:
      raise TaskMuxer.TaskExistsError('Already monitoring %s' % task_id)
    self._tasks[task_id] = TaskMonitor(self._pathspec, task_id)

  @Lockable.sync
  def remove(self, task_id):
    """
      Remove a task_id from the monitored collection.

      If the task does not exist, raises UnknownTaskError
    """
    if task_id not in self._tasks:
      raise TaskMuxer.UnknownTaskError('Tried to remove unmonitored task %s' % task_id)
    self._tasks.pop(task_id)

  @Lockable.sync
  def get_state(self, task_id):
    """
      Get the latest state of the task_id.

      If the task_id is unmonitored, raises UnknownTaskError.
    """
    if task_id not in self._tasks:
      raise TaskMuxer.UnknownTaskError('Not monitoring %s' % task_id)
    return self._tasks[task_id].get_state()

  @Lockable.sync
  def get_active_processes(self):
    """
      Get active processes.  Returned is a list of tuples of the form:
        (task_id, ProcessStatus object of running object, its run number)
    """
    active_processes = []
    for task_id in self._tasks:
      for process_tuple in self._tasks[task_id].get_active_processes():
        active_processes.append((task_id,) + process_tuple)
    return active_processes
