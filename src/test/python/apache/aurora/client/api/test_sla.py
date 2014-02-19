#
# Copyright 2014 Apache Software Foundation
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
import time

from apache.aurora.client.api.sla import Sla, JobUpTimeSlaVector
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.AuroraSchedulerManager import Client as scheduler_client
from gen.apache.aurora.constants import ACTIVE_STATES
from gen.apache.aurora.ttypes import (
    AssignedTask,
    Identity,
    Quota,
    Response,
    ResponseCode,
    Result,
    ScheduleStatus,
    ScheduleStatusResult,
    ScheduledTask,
    TaskConfig,
    TaskEvent,
    TaskQuery
)

from mock import Mock


class SlaTest(unittest.TestCase):
  def setUp(self):
    self._scheduler = Mock()
    self._sla = Sla(self._scheduler)
    self._role = 'mesos'
    self._name = 'job'
    self._env = 'test'
    self._job_key = AuroraJobKey('foo', self._role, self._env, self._name)

  def mock_get_tasks(self, tasks, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, message='test')
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    self._scheduler.getTasksStatus.return_value = resp

  def create_task(self, duration, id):
    return ScheduledTask(
        assignedTask=AssignedTask(instanceId=id, task=TaskConfig(production=True)),
        status=ScheduleStatus.RUNNING,
        taskEvents=[TaskEvent(
            status=ScheduleStatus.STARTING,
            timestamp=(time.time() - duration) * 1000)]
    )

  def create_tasks(self, durations):
    return [self.create_task(duration, index) for index, duration in enumerate(durations)]

  def assert_count_result(self, percentage, duration):
    vector = self._sla.get_job_uptime_vector(self._job_key)
    actual = vector.get_task_up_count(duration)
    assert percentage == actual, (
        'Expected percentage:%s Actual percentage:%s' % (percentage, actual)
    )
    self.expect_task_status_call()

  def assert_uptime_result(self, expected, percentile):
    vector = self._sla.get_job_uptime_vector(self._job_key)
    try:
      actual = vector.get_job_uptime(percentile)
    except ValueError:
      assert expected is None, 'Unexpected error raised.'
    else:
      assert expected is not None, 'Expected error not raised.'
      assert expected == actual, (
          'Expected uptime:%s Actual uptime:%s' % (expected, actual)
      )
      self.expect_task_status_call()

  def expect_task_status_call(self):
    self._scheduler.getTasksStatus.assert_called_once_with(
        TaskQuery(
            owner=Identity(role=self._role),
            environment=self._env,
            jobName=self._name,
            statuses=ACTIVE_STATES)
    )


  def test_count_0(self):
    self.mock_get_tasks([])
    self.assert_count_result(0, 0)

  def test_count_50(self):
    self.mock_get_tasks(self.create_tasks([600, 900, 100, 200]))
    self.assert_count_result(50, 300)

  def test_count_100(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400, 500]))
    self.assert_count_result(100, 50)

  def test_uptime_empty(self):
    self.mock_get_tasks([])
    self.assert_uptime_result(0, 50)

  def test_uptime_0(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(None, 0)

  def test_uptime_10(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(400, 10)

  def test_uptime_50(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(200, 50)

  def test_uptime_99(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(100, 99)

  def test_uptime_100(self):
    self.mock_get_tasks(self.create_tasks([100, 200, 300, 400]))
    self.assert_uptime_result(None, 100)
