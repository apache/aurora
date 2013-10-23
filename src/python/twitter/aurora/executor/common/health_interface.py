from abc import abstractproperty

from twitter.common.lang import Interface
from twitter.common.metrics import LambdaGauge, Observable


class FailureState(object):
  FAILED = object()
  KILLED = object()
  ALL_STATES = {
    FAILED: 'FAILED',
    KILLED: 'KILLED',
  }


class FailureReason(object):
  """
    Encapsulates a reason for failure and an optional reason which defaults to
    FailureState.FAILED.
  """

  def __init__(self, reason, status=FailureState.FAILED):
    self._reason = reason
    if status not in FailureState.ALL_STATES:
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
        FailureState.ALL_STATES[self._status])


class HealthInterface(Observable, Interface):
  @abstractproperty
  def healthy(self):
    pass

  @property
  def failure_reason(self):
    if self.healthy:
      return None
    return FailureReason("Unhealthy")

  def start(self):
    self.metrics.register(LambdaGauge('health', lambda: int(self.healthy)))

  def stop(self):
    pass


class Healthy(HealthInterface):
  @property
  def healthy(self):
    return True
