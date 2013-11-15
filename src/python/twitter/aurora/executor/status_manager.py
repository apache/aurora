import time

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time

from .common.status_checker import StatusChecker


class StatusManager(ExceptionalThread):
  """
    An agent that periodically checks the health of a task via StatusCheckers that
    provide HTTP health checking, resource consumption, etc.

    If any of the status interfaces return a status, the Status Manager
    invokes the user-supplied callback with the status.
  """
  POLL_WAIT = Amount(500, Time.MILLISECONDS)

  def __init__(self, status_checker, callback, clock=time):
    if not isinstance(status_checker, StatusChecker):
      raise TypeError('status_checker must be a StatusChecker, got %s' % type(status_checker))
    if not callable(callback):
      raise TypeError('callback needs to be callable!')
    self._status_checker = status_checker
    self._callback = callback
    self._clock = clock
    super(StatusManager, self).__init__()
    self.daemon = True

  def run(self):
    while True:
      status_result = self._status_checker.status
      if status_result is not None:
        log.info('Status manager got %s' % status_result)
        self._callback(status_result)
        break
      else:
        self._clock.sleep(self.POLL_WAIT.as_(Time.SECONDS))
