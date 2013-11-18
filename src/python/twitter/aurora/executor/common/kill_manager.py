from .status_checker import ExitState, StatusChecker, StatusResult


class KillManager(StatusChecker):
  """
    A health interface that provides a kill-switch for a task monitored by the status manager.
  """
  def __init__(self):
    self._killed = False
    self._reason = None

  @property
  def status(self):
    if self._killed:
      return StatusResult(self._reason, ExitState.KILLED)

  def kill(self, reason):
    self._reason = reason
    self._killed = True
