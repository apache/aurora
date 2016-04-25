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

import unittest
from math import ceil

import mox

from apache.aurora.client.api.health_check import HealthCheck
from apache.aurora.client.api.instance_watcher import InstanceWatcher

from gen.apache.aurora.api.AuroraSchedulerManager import Client as scheduler_client
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    JobKey,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)


class FakeClock(object):
  def __init__(self):
    self._now_seconds = 0.0

  def time(self):
    return self._now_seconds

  def sleep(self, seconds):
    self._now_seconds += seconds


class FakeEvent(object):
  def __init__(self, clock):
    self._clock = clock
    self._is_set = False

  def wait(self, seconds):
    self._clock.sleep(seconds)

  def is_set(self):
    return self._is_set

  def set(self):
    self._is_set = True


def find_expected_cycles(period, sleep_secs):
  return ceil(period / sleep_secs) + 1


class InstanceWatcherTest(unittest.TestCase):
  WATCH_INSTANCES = range(3)
  WATCH_SECS = 50
  EXPECTED_CYCLES = find_expected_cycles(WATCH_SECS, 3.0)

  def setUp(self):
    self._clock = FakeClock()
    self._event = FakeEvent(self._clock)
    self._scheduler = mox.MockObject(scheduler_client)
    self._job_key = JobKey(role='mesos', name='jimbob', environment='test')
    self._health_check = mox.MockObject(HealthCheck)
    self._watcher = InstanceWatcher(self._scheduler,
                                 self._job_key,
                                 self.WATCH_SECS,
                                 health_check_interval_seconds=3,
                                 clock=self._clock,
                                 terminating_event=self._event)

  def get_tasks_status_query(self, instance_ids):
    query = TaskQuery()
    query.jobKeys = set([self._job_key])
    query.statuses = set([ScheduleStatus.RUNNING])
    query.instanceIds = set(instance_ids)
    return query

  def create_task(self, instance_id):
    return ScheduledTask(
        assignedTask=AssignedTask(instanceId=instance_id, task=TaskConfig()))

  def expect_get_statuses(self, instance_ids=WATCH_INSTANCES, num_calls=EXPECTED_CYCLES):
    tasks = [self.create_task(instance_id) for instance_id in instance_ids]
    response = Response(
        responseCode=ResponseCode.OK,
        details=[ResponseDetail(message='test')],
        result=Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks)))

    query = self.get_tasks_status_query(instance_ids)
    for _ in range(int(num_calls)):
      self._scheduler.getTasksWithoutConfigs(query).AndReturn(response)

  def expect_io_error_in_get_statuses(self, instance_ids=WATCH_INSTANCES,
      num_calls=EXPECTED_CYCLES):

    query = self.get_tasks_status_query(instance_ids)
    for _ in range(int(num_calls)):
      self._scheduler.getTasksWithoutConfigs(query).AndRaise(IOError('oops'))

  def mock_health_check(self, task, status):
    self._health_check.health(task).InAnyOrder().AndReturn(status)

  def expect_health_check(self, instance, status, num_calls=EXPECTED_CYCLES):
    num_calls = num_calls if status else 1
    for _ in range(int(num_calls)):
      self.mock_health_check(self.create_task(instance), status)

  def assert_watch_result(self, expected_failed_instances, instances_to_watch=WATCH_INSTANCES):
    instances_returned = self._watcher.watch(instances_to_watch, self._health_check)
    assert set(expected_failed_instances) == instances_returned, (
        'Expected instances (%s) : Returned instances (%s)' % (
            expected_failed_instances, instances_returned))

  def replay_mocks(self):
    mox.Replay(self._scheduler)
    mox.Replay(self._health_check)

  def verify_mocks(self):
    mox.Verify(self._scheduler)
    mox.Verify(self._health_check)

  def test_successful_watch(self):
    """All instances are healthy immediately"""
    self.expect_get_statuses()
    self.expect_health_check(0, True)
    self.expect_health_check(1, True)
    self.expect_health_check(2, True)
    self.replay_mocks()
    self.assert_watch_result([])
    self.verify_mocks()

  def test_single_instance_failure(self):
    """One failed instance in a batch of instances"""
    self.expect_get_statuses()
    self.expect_health_check(0, False)
    self.expect_health_check(1, True)
    self.expect_health_check(2, True)
    self.replay_mocks()
    self.assert_watch_result([0])
    self.verify_mocks()

  def test_all_instance_failure(self):
    """All failed instance in a batch of instances"""
    self.expect_get_statuses(num_calls=1)
    self.expect_health_check(0, False)
    self.expect_health_check(1, False)
    self.expect_health_check(2, False)
    self.replay_mocks()
    self.assert_watch_result([0, 1, 2])
    self.verify_mocks()

  def test_watch_period_failure(self):
    """Instances are reported unhealthy before watch_secs expires"""
    self.expect_get_statuses()
    self.expect_health_check(0, True, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_health_check(1, True, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_health_check(2, True, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_health_check(0, False)
    self.expect_health_check(1, False)
    self.expect_health_check(2, False)
    self.replay_mocks()
    self.assert_watch_result([0, 1, 2])
    self.verify_mocks()

  def test_single_watch_period_failure(self):
    """One instance is reported unhealthy before watch_secs expires"""
    self.expect_get_statuses()
    self.expect_health_check(0, True)
    self.expect_health_check(1, True)
    self.expect_health_check(2, True, num_calls=self.EXPECTED_CYCLES - 1)
    self.expect_health_check(2, False)
    self.replay_mocks()
    self.assert_watch_result([2])
    self.verify_mocks()

  def test_terminated_exits_immediately(self):
    """Terminated instance watched should bail out immediately."""
    self.replay_mocks()
    self._watcher.terminate()
    result = self._watcher.watch([], self._health_check)
    assert result is None, ('Expected instances None : Returned instances (%s)' % result)
    self.verify_mocks()
