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

import mock
from mock import create_autospec

from apache.aurora.client.api.job_monitor import JobMonitor
from apache.aurora.common.aurora_job_key import AuroraJobKey

from ...api_util import SchedulerProxyApiSpec

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
    TaskEvent,
    TaskQuery
)


class FakeEvent(object):
  def __init__(self):
    self._is_set = False

  def wait(self, seconds):
    pass

  def is_set(self):
    return self._is_set

  def set(self):
    self._is_set = True


class JobMonitorTest(unittest.TestCase):

  def setUp(self):
    self._scheduler = create_autospec(spec=SchedulerProxyApiSpec, instance=True)
    self._job_key = AuroraJobKey('cl', 'johndoe', 'test', 'test_job')
    self._event = FakeEvent()

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
    resp = Response(responseCode=response_code, details=[ResponseDetail(message='test')])
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    self._scheduler.getTasksWithoutConfigs.return_value = resp

  def expect_task_status(self, once=False, instances=None):
    query = TaskQuery(jobKeys=[
        JobKey(role=self._job_key.role, environment=self._job_key.env, name=self._job_key.name)])
    if instances is not None:
      query.instanceIds = frozenset([int(s) for s in instances])

    if once:
      self._scheduler.getTasksWithoutConfigs.assert_called_once_with(query, retry=False)
    else:
      self._scheduler.getTasksWithoutConfigs.assert_called_with(query, retry=False)

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

    monitor = JobMonitor(self._scheduler, self._job_key, terminating_event=self._event)
    assert not monitor.wait_until(monitor.terminal, with_timeout=True)
    self.expect_task_status()

  def test_terminated_exits_immediately(self):
    self._event.set()
    monitor = JobMonitor(self._scheduler, self._job_key, terminating_event=self._event)
    assert monitor.wait_until(monitor.terminal)

  def test_terminate(self):
    mock_event = mock.Mock()
    monitor = JobMonitor(self._scheduler, self._job_key, terminating_event=mock_event)
    monitor.terminate()
    mock_event.set.assert_called_once_with()
