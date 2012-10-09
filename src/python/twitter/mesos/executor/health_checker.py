import threading
import time

from .health_interface import HealthInterface, FailureReason


class HealthCheckerThread(HealthInterface, threading.Thread):
  def __init__(self, http_signaler, interval_secs=30, initial_interval_secs=None, clock=time):
    self._checker = http_signaler
    self._interval = interval_secs
    if initial_interval_secs is not None:
      self._initial_interval = initial_interval_secs
    else:
      self._initial_interval = interval_secs * 2
    self._dead = threading.Event()
    self._healthy = True if self._initial_interval > 0 else self._checker()
    self._clock = clock
    super(HealthCheckerThread, self).__init__()
    self.daemon = True

  @property
  def healthy(self):
    return self._healthy

  @property
  def failure_reason(self):
    if not self.healthy:
      return FailureReason('Failed health check!')

  def run(self):
    self._clock.sleep(self._initial_interval)
    while not self._dead.is_set():
      self._healthy = self._checker()
      self._clock.sleep(self._interval)

  def start(self):
    threading.Thread.start(self)

  def stop(self):
    self._dead.set()

