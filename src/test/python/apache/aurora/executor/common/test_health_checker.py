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

import json
import os.path
import threading
import time
import unittest

import mock
import pytest
from mesos.interface.mesos_pb2 import TaskState
from twitter.common.exceptions import ExceptionalThread
from twitter.common.testing.clock import ThreadedClock

from apache.aurora.common.health_check.http_signaler import HttpSignaler
from apache.aurora.config.schema.base import (
    HealthCheckConfig,
    HealthCheckerConfig,
    HttpHealthChecker,
    ShellHealthChecker
)
from apache.aurora.executor.common.health_checker import (
    HealthChecker,
    HealthCheckerProvider,
    NoopHealthChecker
)
from apache.aurora.executor.common.sandbox import SandboxInterface
from apache.aurora.executor.common.status_checker import StatusResult

from .fixtures import HELLO_WORLD, MESOS_JOB

from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, JobKey, TaskConfig


class TestHealthChecker(unittest.TestCase):
  def setUp(self):
    self._clock = ThreadedClock(0)
    self._checker = mock.Mock(spec=HttpSignaler)
    self.initial_interval_secs = 15
    self.interval_secs = 10
    self.fake_health_checks = []
    def mock_health_check():
      return self.fake_health_checks.pop(0)
    self._checker.health = mock.Mock(spec=self._checker.__call__)
    self._checker.health.side_effect = mock_health_check

  def append_health_checks(self, status, num_calls=1):
    for i in range(num_calls):
      self.fake_health_checks.append((status, 'reason'))

  def test_grace_period_2x_success(self):
    '''Grace period is 2 x interval and health checks succeed.'''

    self.append_health_checks(True, num_calls=2)
    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              clock=self._clock)
    hct.start()
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    hct.stop()
    assert self._checker.health.call_count == 1

  def test_grace_period_2x_failure_then_success(self):
    '''Grace period is 2 x interval and health checks fail then succeed.'''

    self.append_health_checks(False)
    self.append_health_checks(True)
    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              clock=self._clock)
    hct.start()
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
    assert hct.threaded_health_checker.running is False
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    hct.stop()
    assert self._checker.health.call_count == 2

  def test_ignore_failures_after_running_inside_grace_period(self):
    '''Grace period is 2 x interval and health checks succeed then fail.'''

    self.append_health_checks(True)
    self.append_health_checks(False)
    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              clock=self._clock)
    hct.start()
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    assert hct.threaded_health_checker.current_consecutive_failures == 0
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    assert hct.threaded_health_checker.current_consecutive_failures == 0
    hct.stop()
    assert self._checker.health.call_count == 2

  def test_does_not_ignores_failures_after_running_outside_grace_period(self):
    '''Grace period is 2 x interval and health checks succeed then fail.'''

    self.append_health_checks(True)
    self.append_health_checks(False, num_calls=2)
    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              clock=self._clock)
    hct.start()
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    assert hct.threaded_health_checker.current_consecutive_failures == 0
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    assert hct.threaded_health_checker.current_consecutive_failures == 0
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Failed health check! reason', TaskState.Value('TASK_FAILED'))
    assert hct.threaded_health_checker.running is True
    assert hct.threaded_health_checker.current_consecutive_failures == 1
    hct.stop()
    assert self._checker.health.call_count == 3

  def test_grace_period_2x_failure(self):
    '''
      Grace period is 2 x interval and all health checks fail.
      Failures are ignored when in grace period.
    '''

    self.append_health_checks(False, num_calls=3)
    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              clock=self._clock)
    hct.start()
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
    assert hct.threaded_health_checker.running is False
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
    assert hct.threaded_health_checker.running is False
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Failed health check! reason', TaskState.Value('TASK_FAILED'))
    assert hct.threaded_health_checker.running is False
    hct.stop()
    assert self._checker.health.call_count == 3

  def test_success_outside_grace_period(self):
    '''
    Health checks fail inside grace period, but pass outside and leads to success
    '''

    self.append_health_checks(False, num_calls=2)
    self.append_health_checks(True)
    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              clock=self._clock)
    hct.start()
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
    assert hct.threaded_health_checker.running is False
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
    assert hct.threaded_health_checker.running is False
    self._clock.tick(self.interval_secs)
    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    hct.stop()
    assert self._checker.health.call_count == 3

  def test_include_max_failure_to_forgiving_attempts(self):
    '''
    Health checks fail but never breaches `max_consecutive_failures`
    '''
    max_consecutive_failures = 4

    # health checks fail within grace period
    self.append_health_checks(False, num_calls=2)

    # health checks fails max_consecutive_failures times and then succeeds
    self.append_health_checks(False, num_calls=max_consecutive_failures)
    self.append_health_checks(True)

    # health checks fails max_consecutive_failures times and then succeeds
    self.append_health_checks(False, num_calls=max_consecutive_failures)
    self.append_health_checks(False)

    hct = HealthChecker(
              self._checker.health,
              interval_secs=self.interval_secs,
              max_consecutive_failures=max_consecutive_failures,
              clock=self._clock)
    hct.start()

    # failures ignored inside grace period
    for _ in range(2):
      assert self._clock.converge(threads=[hct.threaded_health_checker])
      self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
      assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
      assert hct.threaded_health_checker.running is False
      self._clock.tick(self.interval_secs)

    # failures never breach max
    for _ in range(max_consecutive_failures):
      assert self._clock.converge(threads=[hct.threaded_health_checker])
      self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
      assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
      assert hct.threaded_health_checker.running is False
      self._clock.tick(self.interval_secs)

    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.threaded_health_checker.running is True
    self._clock.tick(self.interval_secs)

    # failures breach max, causes task failure
    for _ in range(max_consecutive_failures):
      assert self._clock.converge(threads=[hct.threaded_health_checker])
      self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
      assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
      assert hct.threaded_health_checker.running is True
      self._clock.tick(self.interval_secs)

    assert self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Failed health check! reason', TaskState.Value('TASK_FAILED'))
    assert hct.threaded_health_checker.running is True

    hct.stop()
    assert self._checker.health.call_count == 12

  def test_initial_interval_whatev(self):
    self.append_health_checks(False, 2)
    hct = HealthChecker(
        self._checker.health,
        interval_secs=self.interval_secs,
        grace_period_secs=0,
        clock=self._clock)
    hct.start()
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, self.interval_secs)
    assert hct.status == StatusResult('Failed health check! reason', TaskState.Value('TASK_FAILED'))
    hct.stop()
    assert self._checker.health.call_count == 1

  def test_consecutive_failures_max_failures(self):
    '''Verify that a task is unhealthy after max_consecutive_failures is exceeded'''
    grace_period_secs = self.initial_interval_secs
    interval_secs = self.interval_secs
    self.append_health_checks(True, num_calls=2)
    self.append_health_checks(False, num_calls=3)
    hct = HealthChecker(
        self._checker.health,
        interval_secs=interval_secs,
        grace_period_secs=grace_period_secs,
        max_consecutive_failures=2,
        min_consecutive_successes=2,
        clock=self._clock)
    hct.start()

    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
    assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
    assert hct.metrics.sample()['consecutive_failures'] == 0
    self._clock.tick(interval_secs)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.metrics.sample()['consecutive_failures'] == 0
    assert hct.threaded_health_checker.running is True
    self._clock.tick(interval_secs)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.metrics.sample()['consecutive_failures'] == 1
    self._clock.tick(interval_secs)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
    assert hct.status == StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
    assert hct.metrics.sample()['consecutive_failures'] == 2
    self._clock.tick(interval_secs)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
    assert hct.status == StatusResult('Failed health check! reason', TaskState.Value('TASK_FAILED'))
    assert hct.metrics.sample()['consecutive_failures'] == 3
    hct.stop()
    assert self._checker.health.call_count == 5

  def test_consecutive_failures_failfast(self):
    '''Verify that health check is failed fast'''
    grace_period_secs = self.initial_interval_secs
    interval_secs = self.interval_secs
    self.append_health_checks(False, num_calls=5)
    hct = HealthChecker(
        self._checker.health,
        interval_secs=interval_secs,
        grace_period_secs=grace_period_secs,
        max_consecutive_failures=2,
        min_consecutive_successes=4,
        clock=self._clock)
    hct.start()

    # failure is ignored inside grace_period_secs
    for _ in range(2):
      self._clock.converge(threads=[hct.threaded_health_checker])
      self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
      assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
      assert hct.metrics.sample()['consecutive_failures'] == 0
      self._clock.tick(interval_secs)

    # 3 consecutive health check failures causes fail-fast
    for attempt in range(2):
      self._clock.converge(threads=[hct.threaded_health_checker])
      self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
      assert hct.status == StatusResult(None, TaskState.Value('TASK_STARTING'))
      # failure is not ignored outside grace_period_secs
      assert hct.metrics.sample()['consecutive_failures'] == (attempt + 1)
      self._clock.tick(interval_secs)

    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, interval_secs)
    assert hct.status == StatusResult('Failed health check! reason', TaskState.Value('TASK_FAILED'))
    assert hct.metrics.sample()['consecutive_failures'] == 3
    hct.stop()
    assert self._checker.health.call_count == 5

  @pytest.mark.skipif('True', reason='Flaky test (AURORA-1182)')
  def test_health_checker_metrics(self):
    def slow_check():
      self._clock.sleep(0.5)
      return (True, None)
    hct = HealthChecker(slow_check, interval_secs=1, grace_period_secs=1, clock=self._clock)
    hct.start()
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, amount=1)

    assert hct._total_latency == 0
    assert hct.metrics.sample()['total_latency_secs'] == 0

    # start the health check (during health check it is still 0)
    epsilon = 0.001
    self._clock.tick(1.0 + epsilon)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, amount=0.5)
    assert hct._total_latency == 0
    assert hct.metrics.sample()['total_latency_secs'] == 0
    assert hct.metrics.sample()['checks'] == 0

    # finish the health check
    self._clock.tick(0.5 + epsilon)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, amount=1)  # interval_secs
    assert hct._total_latency == 0.5
    assert hct.metrics.sample()['total_latency_secs'] == 0.5
    assert hct.metrics.sample()['checks'] == 1

    # tick again
    self._clock.tick(1.0 + epsilon)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.tick(0.5 + epsilon)
    self._clock.converge(threads=[hct.threaded_health_checker])
    self._clock.assert_waiting(hct.threaded_health_checker, amount=1)  # interval_secs
    assert hct._total_latency == 1.0
    assert hct.metrics.sample()['total_latency_secs'] == 1.0
    assert hct.metrics.sample()['checks'] == 2


class TestHealthCheckerProvider(unittest.TestCase):
  def test_from_assigned_task_http(self):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    task_config = TaskConfig(
        executorConfig=ExecutorConfig(
            name='thermos',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    min_consecutive_successes=min_consecutive_successes,
                    timeout_secs=7
                )
            ).json_dumps()
        )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'health': 9001})
    health_checker = HealthCheckerProvider().from_assigned_task(assigned_task, None)
    hc = health_checker.threaded_health_checker
    assert hc.interval == interval_secs
    assert hc.grace_period_secs == initial_interval_secs
    assert hc.max_consecutive_failures == max_consecutive_failures
    assert hc.min_consecutive_successes == min_consecutive_successes

  def test_from_assigned_task_http_endpoint_style_config(self):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    http_config = HttpHealthChecker(
      endpoint='/foo',
      expected_response='bar',
      expected_response_code=201
    )
    task_config = TaskConfig(
        executorConfig=ExecutorConfig(
            name='thermos',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    health_checker=HealthCheckerConfig(http=http_config),
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    min_consecutive_successes=min_consecutive_successes,
                    timeout_secs=7
                )
            ).json_dumps()
        )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'health': 9001})
    execconfig_data = json.loads(assigned_task.task.executorConfig.data)
    http_exec_config = execconfig_data['health_check_config']['health_checker']['http']
    assert http_exec_config['endpoint'] == '/foo'
    assert http_exec_config['expected_response'] == 'bar'
    assert http_exec_config['expected_response_code'] == 201
    health_checker = HealthCheckerProvider().from_assigned_task(assigned_task, None)
    hc = health_checker.threaded_health_checker
    assert hc.interval == interval_secs
    assert hc.grace_period_secs == initial_interval_secs
    assert hc.max_consecutive_failures == max_consecutive_failures
    assert hc.min_consecutive_successes == min_consecutive_successes

  @mock.patch('pwd.getpwnam')
  def test_from_assigned_task_shell(self, mock_getpwnam):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    timeout_secs = 5
    shell_config = ShellHealthChecker(shell_command='failed command')
    task_config = TaskConfig(
        job=JobKey(role='role', environment='env', name='name'),
        executorConfig=ExecutorConfig(
            name='thermos-generic',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    health_checker=HealthCheckerConfig(shell=shell_config),
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    min_consecutive_successes=min_consecutive_successes,
                    timeout_secs=timeout_secs
                )
            ).json_dumps()
        )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'foo': 9001})
    execconfig_data = json.loads(assigned_task.task.executorConfig.data)
    assert execconfig_data[
             'health_check_config']['health_checker']['shell']['shell_command'] == 'failed command'

    mock_sandbox = mock.Mock(spec_set=SandboxInterface)
    type(mock_sandbox).root = mock.PropertyMock(return_value='/some/path')
    type(mock_sandbox).is_filesystem_image = mock.PropertyMock(return_value=False)

    health_checker = HealthCheckerProvider().from_assigned_task(assigned_task, mock_sandbox)
    hc = health_checker.threaded_health_checker
    assert hc.interval == interval_secs
    assert hc.grace_period_secs == initial_interval_secs
    assert hc.max_consecutive_failures == max_consecutive_failures
    assert hc.min_consecutive_successes == min_consecutive_successes
    mock_getpwnam.assert_called_once_with(task_config.job.role)

  @mock.patch('pwd.getpwnam')
  def test_from_assigned_task_shell_no_demotion(self, mock_getpwnam):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    timeout_secs = 5
    shell_config = ShellHealthChecker(shell_command='failed command')
    task_config = TaskConfig(
        job=JobKey(role='role', environment='env', name='name'),
        executorConfig=ExecutorConfig(
            name='thermos-generic',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    health_checker=HealthCheckerConfig(shell=shell_config),
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    min_consecutive_successes=min_consecutive_successes,
                    timeout_secs=timeout_secs
                )
            ).json_dumps()
        )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'foo': 9001})
    execconfig_data = json.loads(assigned_task.task.executorConfig.data)
    assert execconfig_data[
             'health_check_config']['health_checker']['shell']['shell_command'] == 'failed command'

    mock_sandbox = mock.Mock(spec_set=SandboxInterface)
    type(mock_sandbox).root = mock.PropertyMock(return_value='/some/path')
    type(mock_sandbox).is_filesystem_image = mock.PropertyMock(return_value=False)

    health_checker = HealthCheckerProvider(nosetuid_health_checks=True).from_assigned_task(
        assigned_task,
        mock_sandbox)
    hc = health_checker.threaded_health_checker
    assert hc.interval == interval_secs
    assert hc.grace_period_secs == initial_interval_secs
    assert hc.max_consecutive_failures == max_consecutive_failures
    assert hc.min_consecutive_successes == min_consecutive_successes
    # Should not be trying to access role's user info.
    assert not mock_getpwnam.called

  @mock.patch('pwd.getpwnam')
  def test_from_assigned_task_shell_filesystem_image(self, mock_getpwnam):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    timeout_secs = 5
    shell_config = ShellHealthChecker(shell_command='failed command')
    task_config = TaskConfig(
            job=JobKey(role='role', environment='env', name='name'),
            executorConfig=ExecutorConfig(
                    name='thermos-generic',
                    data=MESOS_JOB(
                            task=HELLO_WORLD,
                            health_check_config=HealthCheckConfig(
                                    health_checker=HealthCheckerConfig(shell=shell_config),
                                    interval_secs=interval_secs,
                                    initial_interval_secs=initial_interval_secs,
                                    max_consecutive_failures=max_consecutive_failures,
                                    min_consecutive_successes=min_consecutive_successes,
                                    timeout_secs=timeout_secs
                            )
                    ).json_dumps()
            )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'foo': 9001})
    execconfig_data = json.loads(assigned_task.task.executorConfig.data)
    assert execconfig_data[
             'health_check_config']['health_checker']['shell']['shell_command'] == 'failed command'

    mock_sandbox = mock.Mock(spec_set=SandboxInterface)
    type(mock_sandbox).root = mock.PropertyMock(return_value='/some/path')
    type(mock_sandbox).is_filesystem_image = mock.PropertyMock(return_value=True)

    with mock.patch('apache.aurora.executor.common.health_checker.ShellHealthCheck') as mock_shell:
      HealthCheckerProvider(
          nosetuid_health_checks=False,
          mesos_containerizer_path='/some/path/mesos-containerizer').from_assigned_task(
              assigned_task,
              mock_sandbox)

      class NotNone(object):
        def __eq__(self, other):
          return other is not None

      assert mock_shell.mock_calls == [
          mock.call(cmd='failed command', wrapper_fn=NotNone(), preexec_fn=None, timeout_secs=5.0)]

  def test_interpolate_cmd(self):
    """Making sure thermos.ports[foo] gets correctly substituted with assignedPorts info."""
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    timeout_secs = 5
    shell_cmd = 'FOO_PORT={{thermos.ports[foo]}} failed command'
    shell_config = ShellHealthChecker(shell_command=shell_cmd)
    task_config = TaskConfig(
        executorConfig=ExecutorConfig(
            name='thermos-generic',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    health_checker=HealthCheckerConfig(shell=shell_config),
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    min_consecutive_successes=min_consecutive_successes,
                    timeout_secs=timeout_secs
                )
            ).json_dumps()
        )
    )
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'foo': 9001})
    interpolated_cmd = HealthCheckerProvider.interpolate_cmd(
      assigned_task,
      cmd=shell_cmd
    )
    assert interpolated_cmd == 'FOO_PORT=9001 failed command'

  def test_from_assigned_task_no_health_port(self):
    interval_secs = 17
    initial_interval_secs = 3
    max_consecutive_failures = 2
    min_consecutive_successes = 2
    timeout_secs = 5
    task_config = TaskConfig(
        executorConfig=ExecutorConfig(
            name='thermos-generic',
            data=MESOS_JOB(
                task=HELLO_WORLD,
                health_check_config=HealthCheckConfig(
                    interval_secs=interval_secs,
                    initial_interval_secs=initial_interval_secs,
                    max_consecutive_failures=max_consecutive_failures,
                    min_consecutive_successes=min_consecutive_successes,
                    timeout_secs=timeout_secs
                )
            ).json_dumps()
        )
    )
    # No health port and we don't have a shell_command.
    assigned_task = AssignedTask(task=task_config, instanceId=1, assignedPorts={'http': 9001})
    health_checker = HealthCheckerProvider().from_assigned_task(assigned_task, None)
    assert isinstance(health_checker, NoopHealthChecker)


class TestThreadedHealthChecker(unittest.TestCase):
  def setUp(self):
    self.health = mock.Mock()
    self.health.return_value = (True, 'Fake')

    self.sandbox = mock.Mock(spec_set=SandboxInterface)
    self.sandbox.exists.return_value = True
    self.sandbox.root = '/root'

    self.initial_interval_secs = 15
    self.interval_secs = 10
    self.max_consecutive_failures = 1
    self.min_consecutive_successes = 2
    self.clock = mock.Mock(spec=time)
    self.clock.time.return_value = 0

    self.health_checker = HealthChecker(
        self.health,
        None,
        self.interval_secs,
        self.initial_interval_secs,
        self.max_consecutive_failures,
        self.min_consecutive_successes,
        self.clock)
    self.health_checker_sandbox_exists = HealthChecker(
        self.health,
        self.sandbox,
        self.interval_secs,
        self.initial_interval_secs,
        self.max_consecutive_failures,
        self.min_consecutive_successes,
        self.clock)

  def test_perform_check_if_not_disabled_snooze_file_is_none(self):
    self.health_checker_sandbox_exists.threaded_health_checker.snooze_file = None
    assert self.health.call_count == 0
    assert self.health_checker_sandbox_exists.metrics.sample()['snoozed'] == 0
    self.health_checker.threaded_health_checker._perform_check_if_not_disabled()
    assert self.health.call_count == 1
    assert self.health_checker_sandbox_exists.metrics.sample()['snoozed'] == 0

  @mock.patch('os.path', spec_set=os.path)
  def test_perform_check_if_not_disabled_no_snooze_file(self, mock_os_path):
    mock_os_path.isfile.return_value = False
    assert self.health.call_count == 0
    assert self.health_checker_sandbox_exists.metrics.sample()['snoozed'] == 0
    self.health_checker_sandbox_exists.threaded_health_checker._perform_check_if_not_disabled()
    assert self.health.call_count == 1
    assert self.health_checker_sandbox_exists.metrics.sample()['snoozed'] == 0

  @mock.patch('os.path', spec_set=os.path)
  def test_perform_check_if_not_disabled_snooze_file_exists(self, mock_os_path):
    mock_os_path.isfile.return_value = True
    assert self.health.call_count == 0
    assert self.health_checker_sandbox_exists.metrics.sample()['snoozed'] == 0
    result = (
        self.health_checker_sandbox_exists.threaded_health_checker._perform_check_if_not_disabled())
    assert self.health.call_count == 0
    assert self.health_checker_sandbox_exists.metrics.sample()['snoozed'] == 1
    assert result == (True, None)

  def test_maybe_update_health_check_count_reset_count(self):
    hc = self.health_checker.threaded_health_checker
    hc.attempts = hc.forgiving_attempts

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason-1')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason-3')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1

  def test_maybe_update_health_check_count_ignore_failures_within_grace_period(self):
    hc = self.health_checker.threaded_health_checker

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-3')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-4')
    assert hc.current_consecutive_failures == 2
    assert hc.current_consecutive_successes == 0

  def test_maybe_update_health_check_count_dont_ignore_failures_after_grace_period(self):
    hc = self.health_checker.threaded_health_checker
    hc.attempts = hc.forgiving_attempts

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 2
    assert hc.current_consecutive_successes == 0

  def test_maybe_update_health_check_count_fail_fast(self):
    hc = self.health_checker.threaded_health_checker
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.healthy is True
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-3')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-4')
    assert hc.current_consecutive_failures == 2
    assert hc.current_consecutive_successes == 0
    assert hc.running is False
    assert hc.healthy is False
    assert hc.reason == 'reason-4'

  def test_maybe_update_health_check_count_max_failures_1(self):
    hc = self.health_checker.threaded_health_checker
    hc.current_consecutive_successes = 1
    hc.attempts = hc.forgiving_attempts

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1
    assert hc.healthy is True

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0
    assert hc.healthy is True

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 2
    assert hc.current_consecutive_successes == 0
    assert hc.healthy is False
    assert hc.reason == 'reason-2'

  def test_maybe_update_health_check_count_success(self):
    hc = self.health_checker.threaded_health_checker
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.running is False
    assert hc.healthy is True

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1
    assert hc.running is False
    assert hc.healthy is True

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 2
    assert hc.running is True
    assert hc.healthy is True

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 3
    assert hc.running is True
    assert hc.healthy is True

  def test_run_success(self):
    self.health.return_value = (True, 'success')
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert self.clock.sleep.call_count == 3
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 0
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 3
    assert self.health_checker.threaded_health_checker.running is True
    assert self.health_checker.threaded_health_checker.healthy is True
    assert self.health_checker.threaded_health_checker.reason is None

  def test_run_failure(self):
    self.health.return_value = (False, 'failure')
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert self.clock.sleep.call_count == 4
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 2
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is False
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure'

  def test_run_failure_unhealthy_when_failfast(self):
    health_status = [(False, 'failure-1'), (True, None), (False, 'failure-3'), (False, 'failure-4')]
    self.health.side_effect = lambda: health_status.pop(0)
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert self.clock.sleep.call_count == 4
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 2
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is False
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure-4'

  def test_run_unhealthy_after_callback(self):
    health_status = [(True, None), (True, None), (False, 'failure-4'), (False, 'failure-5')]
    self.health.side_effect = lambda: health_status.pop(0)
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert self.clock.sleep.call_count == 4
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 2
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is True
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure-5'

  @mock.patch('apache.aurora.executor.common.health_checker.ExceptionalThread.start',
      spec=ExceptionalThread.start)
  def test_start(self, mock_start):
    assert mock_start.call_count == 0
    self.health_checker.threaded_health_checker.start()
    mock_start.assert_called_once_with(self.health_checker.threaded_health_checker)

  def test_stop(self):
    assert not self.health_checker.threaded_health_checker.dead.is_set()
    self.health_checker.threaded_health_checker.stop()
    assert self.health_checker.threaded_health_checker.dead.is_set()


class TestThreadedHealthCheckerWithDefaults(unittest.TestCase):
  '''
    Similar tests as above but with the default health check configuration. This
    will ensure that the defaults are always valid.
  '''

  def setUp(self):
    self.health = mock.Mock()
    self.health.return_value = (True, 'Fake')

    self.sandbox = mock.Mock(spec_set=SandboxInterface)
    self.sandbox.exists.return_value = True
    self.sandbox.root = '/root'

    self.health_checker = HealthCheckerProvider().from_assigned_task(
        AssignedTask(
            task=TaskConfig(
                executorConfig=ExecutorConfig(
                    name='thermos',
                    data=MESOS_JOB(task=HELLO_WORLD).json_dumps())),
            instanceId=1,
            assignedPorts={'health': 9001}),
        self.sandbox)

    self.health_checker.threaded_health_checker.checker = self.health

  def test_perform_check_if_not_disabled_snooze_file_is_none(self):
    self.health_checker.threaded_health_checker.snooze_file = None
    assert self.health.call_count == 0
    assert self.health_checker.metrics.sample()['snoozed'] == 0
    self.health_checker.threaded_health_checker._perform_check_if_not_disabled()
    assert self.health.call_count == 1
    assert self.health_checker.metrics.sample()['snoozed'] == 0

  @mock.patch('os.path', spec_set=os.path)
  def test_perform_check_if_not_disabled_no_snooze_file(self, mock_os_path):
    mock_os_path.isfile.return_value = False
    assert self.health.call_count == 0
    assert self.health_checker.metrics.sample()['snoozed'] == 0
    self.health_checker.threaded_health_checker._perform_check_if_not_disabled()
    assert self.health.call_count == 1
    assert self.health_checker.metrics.sample()['snoozed'] == 0

  @mock.patch('os.path', spec_set=os.path)
  def test_perform_check_if_not_disabled_snooze_file_exists(self, mock_os_path):
    mock_os_path.isfile.return_value = True
    assert self.health.call_count == 0
    assert self.health_checker.metrics.sample()['snoozed'] == 0
    result = (
        self.health_checker.threaded_health_checker._perform_check_if_not_disabled())
    assert self.health.call_count == 0
    assert self.health_checker.metrics.sample()['snoozed'] == 1
    assert result == (True, None)

  def test_maybe_update_health_check_count_reset_count(self):
    hc = self.health_checker.threaded_health_checker
    hc.attempts = hc.forgiving_attempts

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason-1')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason-3')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1

  def test_maybe_update_health_check_count_ignore_failures_inside_grace_period(self):
    hc = self.health_checker.threaded_health_checker

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

  def test_maybe_update_health_check_count_dont_ignore_failures_after_grace_period(self):
    hc = self.health_checker.threaded_health_checker
    hc.attempts = hc.forgiving_attempts

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 2
    assert hc.current_consecutive_successes == 0

  def test_maybe_update_health_check_count_fail_fast(self):
    hc = self.health_checker.threaded_health_checker
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.healthy is True
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-2')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-3')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0
    assert hc.running is False
    assert hc.healthy is False
    assert hc.reason == 'reason-3'

  def test_maybe_update_health_check_count_max_failures(self):
    hc = self.health_checker.threaded_health_checker
    hc.attempts = hc.forgiving_attempts
    hc.current_consecutive_successes = 1

    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1
    assert hc.healthy is True

    hc.attempts += 1
    hc._maybe_update_health_check_count(False, 'reason-1')
    assert hc.current_consecutive_failures == 1
    assert hc.current_consecutive_successes == 0
    assert hc.healthy is False
    assert hc.reason == 'reason-1'

  def test_maybe_update_health_check_count_success(self):
    hc = self.health_checker.threaded_health_checker
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 0
    assert hc.healthy is True
    assert hc.running is False

    hc.attempts += 1
    hc._maybe_update_health_check_count(True, 'reason')
    assert hc.current_consecutive_failures == 0
    assert hc.current_consecutive_successes == 1
    assert hc.running is True
    assert hc.healthy is True

  @mock.patch('apache.aurora.executor.common.health_checker.time.sleep', spec=time.sleep)
  def test_run_success(self, mock_sleep):
    mock_sleep.return_value = None
    self.health.return_value = (True, 'success')
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert mock_sleep.call_count == 3
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 0
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 3
    assert self.health_checker.threaded_health_checker.running is True
    assert self.health_checker.threaded_health_checker.healthy is True
    assert self.health_checker.threaded_health_checker.reason is None

  @mock.patch('apache.aurora.executor.common.health_checker.time.sleep', spec=time.sleep)
  def test_run_failure(self, mock_sleep):
    mock_sleep.return_value = None
    self.health.return_value = (False, 'failure')
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert mock_sleep.call_count == 3
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 1
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is False
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure'

  @mock.patch('apache.aurora.executor.common.health_checker.time.sleep', spec=time.sleep)
  def test_first_success_after_grace_period_and_max_consecutive_failures(self, mock_sleep):
    mock_sleep.return_value = None
    health_status = [(False, 'failure-1'), (False, 'failure-2'), (True, None)]
    self.health.side_effect = lambda: health_status.pop(0)
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert mock_sleep.call_count == 3
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 0
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 1
    assert self.health_checker.threaded_health_checker.running is True
    assert self.health_checker.threaded_health_checker.healthy is True
    assert self.health_checker.threaded_health_checker.reason is None

  @mock.patch('apache.aurora.executor.common.health_checker.time.sleep', spec=time.sleep)
  def test_success_then_failures_ignored_till_grace_period_ends(self, mock_sleep):
    mock_sleep.return_value = None
    health_status = [(True, None), (False, 'failure-2'), (False, 'failure-3')]
    self.health.side_effect = lambda: health_status.pop(0)
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert mock_sleep.call_count == 3
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 1
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is True
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure-3'

  @mock.patch('apache.aurora.executor.common.health_checker.time.sleep', spec=time.sleep)
  def test_run_failure_unhealthy_when_failfast(self, mock_sleep):
    mock_sleep.return_value = None
    health_status = [(False, 'failure-1'), (False, 'failure-2'), (False, 'failure-3')]
    self.health.side_effect = lambda: health_status.pop(0)
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert mock_sleep.call_count == 3
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 1
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is False
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure-3'

  @mock.patch('apache.aurora.executor.common.health_checker.time.sleep', spec=time.sleep)
  def test_run_unhealthy_after_callback(self, mock_sleep):
    mock_sleep.return_value = None
    health_status = [(True, None), (True, None), (False, 'failure-4'), (False, 'failure-5')]
    self.health.side_effect = lambda: health_status.pop(0)
    mock_is_set = mock.Mock(spec=threading._Event.is_set)
    liveness = [False, False, False, False, True]
    mock_is_set.side_effect = lambda: liveness.pop(0)
    self.health_checker.threaded_health_checker.dead.is_set = mock_is_set
    self.health_checker.threaded_health_checker.run()
    assert mock_sleep.call_count == 4
    assert self.health_checker.threaded_health_checker.current_consecutive_failures == 2
    assert self.health_checker.threaded_health_checker.current_consecutive_successes == 0
    assert self.health_checker.threaded_health_checker.running is True
    assert self.health_checker.threaded_health_checker.healthy is False
    assert self.health_checker.threaded_health_checker.reason == 'failure-5'

  @mock.patch('apache.aurora.executor.common.health_checker.ExceptionalThread.start',
      spec=ExceptionalThread.start)
  def test_start(self, mock_start):
    assert mock_start.call_count == 0
    self.health_checker.threaded_health_checker.start()
    mock_start.assert_called_once_with(self.health_checker.threaded_health_checker)

  def test_stop(self):
    assert not self.health_checker.threaded_health_checker.dead.is_set()
    self.health_checker.threaded_health_checker.stop()
    assert self.health_checker.threaded_health_checker.dead.is_set()
