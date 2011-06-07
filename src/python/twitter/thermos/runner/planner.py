__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class Planner:
  """
     Given a scheduler, determine runnable tasks based upon what is running
     and what has finished.
  """

  def __init__(self, scheduler):
    self._scheduler = scheduler
    self.finished   = set([])
    self.running    = set([])

    if not scheduler.timetable():
      raise Exception("Planner was given invalid schedule!")

  def get_runnable(self):
    runnable = self._scheduler.runnable(self.finished)
    return set(runnable) - self.running

  def get_running(self):
    return [r for r in self.running]

  # TODO(wickman) regression tests here.
  def forget(self, task):
    if self.is_finished(task): self.finished.remove(task)
    if self.is_running(task):  self.running.remove(task)

  def set_running(self, task):
    if self.is_finished(task):    self.finished.remove(task)
    if not self.is_running(task): self.running.add(task)

  def set_finished(self, task):
    if self.is_running(task):      self.running.remove(task)
    if not self.is_finished(task): self.finished.add(task)

  def is_finished(self, task):
    return (task in self.finished)

  def is_running(self, task):
    return (task in self.running)

  def is_complete(self):
    return len(self.finished) == len(self._scheduler._all)
