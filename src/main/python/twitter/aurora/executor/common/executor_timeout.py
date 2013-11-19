from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.exceptions import ExceptionalThread


class ExecutorTimeout(ExceptionalThread):
  DEFAULT_TIMEOUT = Amount(10, Time.SECONDS)

  def __init__(self, event, driver, logger=log.error, timeout=DEFAULT_TIMEOUT):
    self._event = event
    self._logger = logger
    self._driver = driver
    self._timeout = timeout
    super(ExecutorTimeout, self).__init__()
    self.daemon = True

  def run(self):
    self._event.wait(self._timeout.as_(Time.SECONDS))
    if not self._event.is_set():
      self._logger('Executor timing out.')
      self._driver.stop()
