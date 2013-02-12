import threading
import time

from twitter.common.exceptions import ExceptionalThread
from .health_interface import HealthInterface, FailureReason


class HealthCheckerThread(HealthInterface, ExceptionalThread):
  """Generic, HealthInterface-conforming thread for arbitrary periodic health checks

    health_checker should be a callable returning a tuple of (boolean, reason), indicating
    respectively the health of the service and the reason for its failure (or None if the service is
    still healthy).

  """
  def __init__(self, health_checker, interval_secs=30, initial_interval_secs=None, clock=time):
    self._checker = health_checker
    self._interval = interval_secs
    if initial_interval_secs is not None:
      self._initial_interval = initial_interval_secs
    else:
      self._initial_interval = interval_secs * 2
    self._dead = threading.Event()
    if self._initial_interval > 0:
      self._healthy, self._reason = True, None
    else:
      self._healthy, self._reason = self._checker()
    self._clock = clock
    super(HealthCheckerThread, self).__init__()
    self.daemon = True

  @property
  def healthy(self):
    return self._healthy

  @property
  def failure_reason(self):
    if not self.healthy:
      return FailureReason('Failed health check! %s' % self._reason)

  def run(self):
    self._clock.sleep(self._initial_interval)
    while not self._dead.is_set():
      self._healthy, self._reason = self._checker()
      self._clock.sleep(self._interval)

  def start(self):
    ExceptionalThread.start(self)

  def stop(self):
    self._dead.set()
