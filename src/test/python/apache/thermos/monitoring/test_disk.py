#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os
from tempfile import mkstemp
from unittest import TestCase

from twitter.common import dirutil
from twitter.common.dirutil import safe_mkdtemp
from twitter.common.quantity import Amount, Data

from apache.thermos.monitoring.disk import DiskCollector

TEST_AMOUNT_1 = Amount(100, Data.MB)
TEST_AMOUNT_2 = Amount(10, Data.MB)
TEST_AMOUNT_SUM = TEST_AMOUNT_1 + TEST_AMOUNT_2


def make_file(size, dir):
  _, filename = mkstemp(dir=dir)
  with open(filename, 'w') as f:
    f.write('0' * int(size.as_(Data.BYTES)))

    # Workaround for AURORA-1956.  On macOS 10.13 with APFS, st_blocks is not
    # consistent with st_size.
    while dirutil.safe_size(filename) < int(size.as_(Data.BYTES)):
      f.write('0' * 1024)
  return filename


def _run_collector_tests(collector, target, wait):
  assert collector.value == 0

  collector.sample()
  wait()
  assert collector.value == 0

  f1 = make_file(TEST_AMOUNT_1, dir=target)
  wait()
  assert collector.value >= TEST_AMOUNT_1.as_(Data.BYTES)

  make_file(TEST_AMOUNT_2, dir=target)
  wait()
  assert collector.value >= TEST_AMOUNT_SUM.as_(Data.BYTES)

  os.unlink(f1)
  wait()
  assert TEST_AMOUNT_SUM.as_(Data.BYTES) > collector.value >= TEST_AMOUNT_2.as_(Data.BYTES)


class TestDiskCollector(TestCase):
  def test_du_diskcollector(self):
    target = safe_mkdtemp()
    collector = DiskCollector(target)

    def wait():
      collector.sample()
      if collector._thread is not None:
        collector._thread.event.wait()

    _run_collector_tests(collector, target, wait)
