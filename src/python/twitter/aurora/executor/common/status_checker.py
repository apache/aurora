from abc import abstractproperty

from twitter.common.lang import Interface
from twitter.common.metrics import NamedGauge, Observable


# This mirrors mesos_pb2 TaskStatus without explicitly depending upon it.
#
# The dependency is a 30MB egg, so for smaller applications that just need
# the status text, we proxy them.  The actual conversion betwen ExitState
# and TaskStatus is done in the StatusManager.
class ExitState(object):
  FAILED = object()
  FINISHED = object()
  KILLED = object()
  LOST = object()

  ALL_STATES = {
    FAILED: 'FAILED',
    FINISHED: 'FINISHED',
    KILLED: 'KILLED',
    LOST: 'LOST',
  }


class StatusResult(object):
  """
    Encapsulates a reason for failure and an optional reason which defaults to
    ExitState.FAILED.
  """

  def __init__(self, reason, status):
    self._reason = reason
    if status not in ExitState.ALL_STATES:
      raise ValueError('Unknown task state: %r' % status)
    self._status = status

  @property
  def reason(self):
    return self._reason

  @property
  def status(self):
    return self._status

  def __repr__(self):
    return '%s(%r, status=%r)' % (
        self.__class__.__name__,
        self._reason,
        ExitState.ALL_STATES[self._status])


class StatusChecker(Observable, Interface):
  @abstractproperty
  def status(self):
    """Return None under normal operations.  Return StatusResult to indicate status proposal."""

  def start(self):
    self.metrics.register(NamedGauge('enabled', 1))

  def stop(self):
    pass


class Healthy(StatusChecker):
  @property
  def status(self):
    return None


class ChainedStatusChecker(StatusChecker):
  def __init__(self, status_checkers):
    self._status_checkers = status_checkers
    self._status = None
    if not all(isinstance(h_i, StatusChecker) for h_i in status_checkers):
      raise TypeError('ChainedStatusChecker must take an iterable of StatusCheckers.')
    super(ChainedStatusChecker, self).__init__()

  @property
  def status(self):
    if self._status is None:
      for status_checker in self._status_checkers:
        status_checker_status = status_checker.status
        if status_checker_status is not None:
          if not isinstance(status_checker_status, StatusResult):
            raise TypeError('StatusChecker returned something other than a StatusResult: got %s' %
                type(status_checker_status))
          self._status = status_checker_status
          break
    return self._status

  def start(self):
    for status_checker in self._status_checkers:
      status_checker.start()

  def stop(self):
    for status_checker in self._status_checkers:
      status_checker.stop()
