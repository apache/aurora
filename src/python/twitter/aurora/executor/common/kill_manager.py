import threading

from .health_interface import ExitReason, ExitState, HealthInterface


class KillManager(HealthInterface):
  """
    A health interface that provides a kill-switch for a task monitored by the status manager.
  """
  def __init__(self):
    self._killed = False
    self._reason = None

  @property
  def healthy(self):
    return not self._killed

  @property
  def exit_reason(self):
    return ExitReason(self._reason, ExitState.KILLED)

  def kill(self, reason):
    self._reason = reason
    self._killed = True
