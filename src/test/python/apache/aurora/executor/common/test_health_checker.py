#
# Copyright 2013 Apache Software Foundation
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

import time
import unittest

from twitter.common.testing.clock import ThreadedClock

from apache.aurora.executor.common.health_checker import HealthCheckerThread

import mox


def thread_yield():
  time.sleep(0.1)

class TestHealthChecker(unittest.TestCase):
  def setUp(self):
    self._clock = ThreadedClock()
    self._mox = mox.Mox()
    self._checker = self._mox.CreateMockAnything()

  def expect_health_check(self, status, num_calls=1):
    for x in range(int(num_calls)):
      self._checker().AndReturn((status, 'reason'))

  def replay(self):
    self._mox.ReplayAll()

  def verify(self):
    self._mox.VerifyAll()

  def test_initial_interval_2x(self):
    self.expect_health_check(False)
    self.replay()
    hct = HealthCheckerThread(self._checker, interval_secs=5, clock=self._clock)
    hct.start()
    thread_yield()
    assert hct.status is None
    self._clock.tick(6)
    assert hct.status is None
    self._clock.tick(3)
    assert hct.status is None
    self._clock.tick(5)
    thread_yield()
    assert hct.status is not None
    hct.stop()
    self.verify()

  def test_initial_interval_whatev(self):
    self.expect_health_check(False)
    self.replay()
    hct = HealthCheckerThread(
      self._checker,
      interval_secs=5,
      initial_interval_secs=0,
      clock=self._clock)
    hct.start()
    assert hct.status is not None
    hct.stop()
    self.verify()

  def test_consecutive_failures(self):
    '''Verify that a task is unhealthy only after max_consecutive_failures is exceeded'''
    initial_interval_secs = 2
    interval_secs = 1
    self.expect_health_check(False, num_calls=2)
    self.expect_health_check(True)
    self.expect_health_check(False, num_calls=3)
    self.replay()
    hct = HealthCheckerThread(
        self._checker,
        interval_secs=interval_secs,
        initial_interval_secs=initial_interval_secs,
        max_consecutive_failures=2,
        clock=self._clock)
    hct.start()

    # 2 consecutive health check failures followed by a successful health check.
    self._clock.tick(initial_interval_secs)
    assert hct.status is None
    self._clock.tick(interval_secs)
    assert hct.status is None
    self._clock.tick(interval_secs)
    assert hct.status is None

    # 3 consecutive health check failures.
    self._clock.tick(interval_secs)
    assert hct.status is None
    self._clock.tick(interval_secs)
    assert hct.status is None
    self._clock.tick(interval_secs)
    thread_yield()
    assert hct.status is not None
    hct.stop()
    self.verify()
