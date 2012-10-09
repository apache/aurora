from abc import ABCMeta, abstractproperty


class FailureReason(object):
  def __init__(self, reason):
    self._reason = reason

  @property
  def reason(self):
    return self._reason

  def __repr__(self):
    return '%s(%r)' % (self.__class__.__name__, self._reason)


class HealthInterface(object):
  __metaclass__ = ABCMeta

  @abstractproperty
  def healthy(self):
    pass

  @property
  def failure_reason(self):
    if self.healthy:
      return None
    return FailureReason("Unhealthy")

  def start(self):
    pass

  def stop(self):
    pass


class Healthy(HealthInterface):
  @property
  def healthy(self):
    return True
