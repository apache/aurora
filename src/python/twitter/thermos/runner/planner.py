__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class Planner(object):
  """
     Given a scheduler, determine runnable processes based upon what is running
     and what has finished.
  """

  class InvalidSchedule(Exception): pass

  def __init__(self, scheduler):
    self._scheduler = scheduler
    self.finished   = set([])
    self.running    = set([])

    if not scheduler.timetable():
      raise Planner.InvalidSchedule("Planner was given an inconsistent schedule!")

  def get_runnable(self):
    runnable = self._scheduler.runnable(self.finished)
    return set(runnable) - self.running

  def get_running(self):
    return list(self.running)

  def get_finished(self):
    return list(self.finished)

  # TODO(wickman) regression tests here.
  def forget(self, process):
    if self.is_finished(process): self.finished.remove(process)
    if self.is_running(process):  self.running.remove(process)

  def set_running(self, process):
    if self.is_finished(process):    self.finished.remove(process)
    if not self.is_running(process): self.running.add(process)

  def set_finished(self, process):
    if self.is_running(process):      self.running.remove(process)
    if not self.is_finished(process): self.finished.add(process)

  def is_finished(self, process):
    return (process in self.finished)

  def is_running(self, process):
    return (process in self.running)

  def is_complete(self):
    return len(self.finished) == len(self._scheduler._all)
