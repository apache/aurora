from abc import abstractmethod

from twitter.common.lang import Interface

from .status_checker import StatusChecker


class TaskError(Exception):
  pass


class TaskRunner(StatusChecker):
  # For now, TaskRunner should just maintain the StatusChecker API.
  pass


class TaskRunnerProvider(Interface):
  @abstractmethod
  def from_assigned_task(self, assigned_task, sandbox):
    pass
