import atexit
import os
from tempfile import mkstemp
import time

from twitter.common.quantity import Amount, Data, Time
from twitter.common.dirutil import safe_mkdtemp
from twitter.thermos.monitoring.disk import DiskCollector
from twitter.thermos.monitoring.disk import InotifyDiskCollector

TEST_AMOUNT_1 = Amount(100, Data.MB)
TEST_AMOUNT_2 = Amount(10, Data.MB)
TEST_AMOUNT_SUM = TEST_AMOUNT_1 + TEST_AMOUNT_2


def make_file(size, dir):
  _, filename = mkstemp(dir=dir)
  with open(filename, 'w') as f:
    f.write('0' * int(size.as_(Data.BYTES)))
  return filename


def _run_collector_tests(collector, target, wait):
  assert collector.value == 0

  collector.sample()
  wait()
  assert collector.value == 0

  f1 = make_file(TEST_AMOUNT_1, dir=target)
  wait()
  assert collector.value >= TEST_AMOUNT_1.as_(Data.BYTES)

  f2 = make_file(TEST_AMOUNT_2, dir=target)
  wait()
  assert collector.value >= TEST_AMOUNT_SUM.as_(Data.BYTES)

  os.unlink(f1)
  wait()
  assert TEST_AMOUNT_SUM.as_(Data.BYTES) > collector.value >= TEST_AMOUNT_2.as_(Data.BYTES)


def test_du_diskcollector():
  target = safe_mkdtemp()
  collector = DiskCollector(target)

  def wait():
    collector.sample()
    if collector._thread is not None:
      collector._thread.event.wait()

  _run_collector_tests(collector, target, wait)


def test_inotify_diskcollector():
  target = safe_mkdtemp()
  INTERVAL = Amount(50, Time.MILLISECONDS)
  collector = InotifyDiskCollector(target)
  collector._thread.COLLECTION_INTERVAL = INTERVAL

  def wait():
    time.sleep((2 * INTERVAL).as_(Time.SECONDS))

  _run_collector_tests(collector, target, wait)
