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

import os.path
import threading
import time
import unittest

import mock
from mesos.interface.mesos_pb2 import TaskState
from twitter.common.exceptions import ExceptionalThread
from twitter.common.testing.clock import ThreadedClock

from apache.aurora.common.http_signaler import HttpSignaler
from apache.aurora.config.schema.base import HealthCheckConfig
from apache.aurora.executor.common.fixtures import HELLO_WORLD, MESOS_JOB
from apache.aurora.executor.common.health_checker import (
    HealthChecker,
    HealthCheckerProvider,
    ThreadedHealthChecker
)
from apache.aurora.executor.common.sandbox import SandboxInterface

from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, TaskConfig


def thread_yield():
  time.sleep(0.1)


class TestHealthChecker(unittest.TestCase):
  def setUp(self):
    self._clock = ThreadedClock()
    self._checker = mock.Mock(spec=HttpSignaler)

    self.fake_health_checks = []
    def mock_health_check():
      return self.fake_health_checks.pop(0)
    self._checker.health = mock.Mock(spec=self._checker.health)
    self._checker.health.side_effect = mock_health_check

  def append_health_checks(self, status, num_calls=1):
    for i in range(num_calls):
      self.fake_health_checks.append((status, 'reason'))

  def test_initial_interval_2x(self):
    self.append_health_checks(False)
    hct = HealthChecker(self._checker.health, interval_secs=5, clock=self._clock)
    hct.start()
    thread_yield()
    assert hct.status is None
    self._clock.tick(6)
    assert hct.status is None
    self._clock.tick(3)
    assert hct.status is None
    self._clock.tick(5)
    thread_yield()
    assert hct.status.status == TaskState.Value('TASK_FAILED')
    hct.stop()
    assert self._checker.health.call_count == 1

  def test_initial_interval_whatev(self):
    self.append_health_checks(False)
    hct = HealthChecker(
      self._checker.health,
      interval_secs=5,
      initial_interval_secs=0,
      clock=self._clock)
    hct.start()
    assert hct.status.status == TaskState.Value('TASK_FAILED')
    hct.stop()
    assert self._checker.health.call_count == 1

  def test_consecutive_failures(self):
    '''Verify that a task is unhealthy only after max_consecutive_failures is exceeded'''
    initial_interval_secs = 2
    interval_secs = 1
    self.append_health_checks(False, num_calls=2)
    self.append_health_checks(True)
    self.append_health_checks(False, num_calls=3)
    hct = HealthChecker(
        self._checker.health,
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
    assert hct.status.status == TaskState.Value('TASK_FAILED')
    hct.stop()
    assert self._checker.health.call_count == 6


class TestHealthCheckerProvider(unittest.TestCase):
  def test_from_assigned_task(self):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    task_config = TaskConfig(
        executorConfig=ExecutorConfig(
            name='thermos',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    timeout_secs=7
                )
            ).json_dumps()
        )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'health': 9001})
    health_checker = HealthCheckerProvider().from_assigned_task(assigned_task, None)
    assert health_checker.threaded_health_checker.interval == interval_secs
    assert health_checker.threaded_health_checker.initial_interval == initial_interval_secs
    hct_max_fail = health_checker.threaded_health_checker.max_consecutive_failures
    assert hct_max_fail == max_consecutive_failures


class TestThreadedHealthChecker(unittest.TestCase):
  def setUp(self):
    self.signaler = mock.Mock(spec=HttpSignaler)
    self.signaler.health.return_value = (True, 'Fake')

    self.sandbox = mock.Mock(spec_set=SandboxInterface)
    self.sandbox.exists.return_value = True
    self.sandbox.root = '/root'

    self.initial_interval_secs = 1
    self.interval_secs = 5
    self.max_consecutive_failures = 2
    self.clock = mock.Mock(spec=time)
    self.threaded_health_checker = ThreadedHealthChecker(
        self.signaler.health,
        None,
        self.interval_secs,
        self.initial_interval_secs,
        self.max_consecutive_failures,
        self.clock)

    self.threaded_health_checker_sandbox_exists = ThreadedHealthChecker(
        self.signaler.health,
        self.sandbox,
        self.interval_secs,
        self.initial_interval_secs,
        self.max_consecutive_failures,
        self.clock)

  def test_perform_check_if_not_disabled_snooze_file_is_none(self):
    self.threaded_health_checker.snooze_file = None

    assert self.signaler.health.call_count == 0
    self.threaded_health_checker._perform_check_if_not_disabled()
    assert self.signaler.health.call_count == 1

  @mock.patch('os.path', spec_set=os.path)
  def test_perform_check_if_not_disabled_no_snooze_file(self, mock_os_path):
    mock_os_path.isfile.return_value = False

    assert self.signaler.health.call_count == 0
    self.threaded_health_checker_sandbox_exists._perform_check_if_not_disabled()
    assert self.signaler.health.call_count == 1

  @mock.patch('os.path', spec_set=os.path)
  def test_perform_check_if_not_disabled_snooze_file_exists(self, mock_os_path):
    mock_os_path.isfile.return_value = True

    assert self.signaler.health.call_count == 0
    result = self.threaded_health_checker_sandbox_exists._perform_check_if_not_disabled()
    assert self.signaler.health.call_count == 0
    assert result == (True, None)

  def test_maybe_update_failure_count(self):
    assert self.threaded_health_checker.current_consecutive_failures == 0
    assert self.threaded_health_checker.healthy is True

    self.threaded_health_checker._maybe_update_failure_count(True, 'reason')
    assert self.threaded_health_checker.current_consecutive_failures == 0

    self.threaded_health_checker._maybe_update_failure_count(False, 'reason')
    assert self.threaded_health_checker.current_consecutive_failures == 1
    assert self.threaded_health_checker.healthy is True

    self.threaded_health_checker._maybe_update_failure_count(False, 'reason')
    assert self.threaded_health_checker.current_consecutive_failures == 2
    assert self.threaded_health_checker.healthy is True

    self.threaded_health_checker._maybe_update_failure_count(False, 'reason')
    assert self.threaded_health_checker.healthy is False
    assert self.threaded_health_checker.reason == 'reason'

  @mock.patch('apache.aurora.executor.common.health_checker.ThreadedHealthChecker'
      '._maybe_update_failure_count',
      spec=ThreadedHealthChecker._maybe_update_failure_count)
  def test_run(self, mock_maybe_update_failure_count):
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    self.threaded_health_checker.dead.is_set = mock_is_set
    liveness = [False, False, True]
    def is_set():
      return liveness.pop(0)
    self.threaded_health_checker.dead.is_set.side_effect = is_set
    self.threaded_health_checker.run()
    assert self.clock.sleep.call_count == 3
    assert mock_maybe_update_failure_count.call_count == 2

  @mock.patch('apache.aurora.executor.common.health_checker.ExceptionalThread.start',
      spec=ExceptionalThread.start)
  def test_start(self, mock_start):
    assert mock_start.call_count == 0
    self.threaded_health_checker.start()
    mock_start.assert_called_once_with(self.threaded_health_checker)

  def test_stop(self):
    assert not self.threaded_health_checker.dead.is_set()
    self.threaded_health_checker.stop()
    assert self.threaded_health_checker.dead.is_set()
