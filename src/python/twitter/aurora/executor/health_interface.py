from abc import abstractproperty

from twitter.common.lang import Interface
from twitter.common.metrics import LambdaGauge, Observable

import mesos_pb2 as mesos_pb


class FailureReason(object):
  """
    Encapsulates a reason for failure and an optional associated core status, which by
    default is mesos_pb2.TASK_FAILED
  """
  def __init__(self, reason, status=mesos_pb.TASK_FAILED):
    self._reason = reason
    if status not in mesos_pb._TASKSTATE.values_by_number:
      raise ValueError('Unknown task state: %r' % status)
    self._status = status

  @property
  def reason(self):
    return self._reason

  @property
  def status(self):
    return self._status

  def __repr__(self):
    return '%s(%r, status=%r)' % (self.__class__.__name__, self._reason,
        mesos_pb._TASKSTATE.values_by_number[self._status].name)


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
