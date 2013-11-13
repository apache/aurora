from abc import abstractproperty

from twitter.common.lang import Interface
from twitter.common.metrics import LambdaGauge, Observable


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


class ExitReason(object):
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


class HealthInterface(Observable, Interface):
  @abstractproperty
  def healthy(self):
    pass

  @property
  def exit_reason(self):
    if self.healthy:
      return None
    return ExitReason("Unhealthy", ExitState.FAILED)

  def start(self):
    self.metrics.register(LambdaGauge('health', lambda: int(self.healthy)))

  def stop(self):
    pass


class Healthy(HealthInterface):
  @property
  def healthy(self):
    return True
