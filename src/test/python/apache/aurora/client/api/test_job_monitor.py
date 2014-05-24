#
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

from mock import Mock

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.AuroraSchedulerManager import Client
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery
)


class FakeClock(object):
  def sleep(self, seconds):
    pass


class JobMonitorTest(unittest.TestCase):

  def setUp(self):
    self._scheduler = Mock()
    self._job_key = AuroraJobKey('cl', 'johndoe', 'test', 'test_job')
    self._clock = FakeClock()

  def create_task(self, status, id):
    return ScheduledTask(
        assignedTask=AssignedTask(
            instanceId=id,
            taskId=id),
        status=status,
        taskEvents=[TaskEvent(
            status=status,
            timestamp=10)]
    )
  def mock_get_tasks(self, tasks, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, message='test')
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    self._scheduler.getTasksStatus.return_value = resp

  def expect_task_status(self, once=False, instances=None):
    query = TaskQuery(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.env,
        jobName=self._job_key.name)
    if instances is not None:
      query.instanceIds = frozenset([int(s) for s in instances])

    if once:
      self._scheduler.getTasksStatus.assert_called_once_with(query)
    else:
      self._scheduler.getTasksStatus.assert_called_with(query)

  def test_wait_until_state(self):
    self.mock_get_tasks([
        self.create_task(ScheduleStatus.RUNNING, '1'),
        self.create_task(ScheduleStatus.RUNNING, '2'),
        self.create_task(ScheduleStatus.FAILED, '3'),
    ])

    monitor = JobMonitor(self._scheduler, self._job_key)
    assert monitor.wait_until(monitor.running_or_finished)
    self.expect_task_status(once=True)

  def test_empty_job_succeeds(self):
    self.mock_get_tasks([])

    monitor = JobMonitor(self._scheduler, self._job_key)
    assert monitor.wait_until(monitor.running_or_finished)
    self.expect_task_status(once=True)

  def test_wait_with_instances(self):
    self.mock_get_tasks([
        self.create_task(ScheduleStatus.FAILED, '2'),
    ])

    monitor = JobMonitor(self._scheduler, self._job_key)
    assert monitor.wait_until(monitor.terminal, instances=[2])
    self.expect_task_status(once=True, instances=[2])

  def test_wait_until_timeout(self):
    self.mock_get_tasks([
        self.create_task(ScheduleStatus.RUNNING, '1'),
        self.create_task(ScheduleStatus.RUNNING, '2'),
        self.create_task(ScheduleStatus.RUNNING, '3'),
    ])

    monitor = JobMonitor(self._scheduler, self._job_key, clock=self._clock)
    assert not monitor.wait_until(monitor.terminal, with_timeout=True)
    self.expect_task_status()
