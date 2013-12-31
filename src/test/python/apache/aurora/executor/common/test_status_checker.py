import threading

from apache.aurora.executor.common.status_checker import (
    ChainedStatusChecker,
    ExitState,
    Healthy,
    StatusChecker,
    StatusResult,
)


class EventHealth(StatusChecker):
  def __init__(self):
    self.started = threading.Event()
    self.stopped = threading.Event()
    self._status = None

  @property
  def status(self):
    return self._status

  def set_status(self, status):
    self._status = status

  def start(self):
    self.started.set()

  def stop(self):
    self.stopped.set()


def test_chained_health_interface():
  hi = ChainedStatusChecker([])
  assert hi.status is None

  hi = ChainedStatusChecker([Healthy()])
  assert hi.status is None

  si1 = EventHealth()
  si2 = EventHealth()
  chained_si = ChainedStatusChecker([si1, si2])

  for si in (si1, si2):
    assert not si.started.is_set()
  chained_si.start()
  for si in (si1, si2):
    assert si.started.is_set()

  assert chained_si.status is None
  reason = StatusResult('derp', ExitState.FAILED)
  si2.set_status(reason)
  assert chained_si.status == reason
  assert chained_si.status.reason == 'derp'
  assert chained_si.status.status == ExitState.FAILED

  for si in (si1, si2):
    assert not si.stopped.is_set()
  chained_si.stop()
  for si in (si1, si2):
    assert si.stopped.is_set()
