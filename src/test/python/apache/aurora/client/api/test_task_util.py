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

from mock import create_autospec

from apache.aurora.client.api.scheduler_mux import SchedulerMux
from apache.aurora.client.api.task_util import StatusMuxHelper

from ..api.api_util import SchedulerThriftApiSpec

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatusResult,
    TaskQuery
)


class TaskUtilTest(unittest.TestCase):
  INSTANCES = [1]

  @classmethod
  def create_query(cls, instances):
    query = TaskQuery()
    query.instanceIds = set(instances)
    return query

  @classmethod
  def create_mux_helper(cls, scheduler, query, scheduler_mux=None):
    return StatusMuxHelper(scheduler, query, scheduler_mux=scheduler_mux)

  @classmethod
  def create_tasks(cls):
    return [ScheduledTask(assignedTask=AssignedTask(instanceId=index)) for index in cls.INSTANCES]

  @classmethod
  def mock_mux(cls, tasks):
    mux = create_autospec(spec=SchedulerMux, instance=True)
    mux.enqueue_and_wait.return_value = tasks
    return mux

  @classmethod
  def mock_scheduler(cls, response_code=None):
    scheduler = create_autospec(spec=SchedulerThriftApiSpec, instance=True)
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, messageDEPRECATED='test')
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=cls.create_tasks()))
    scheduler.getTasksWithoutConfigs.return_value = resp
    return scheduler

  def test_no_mux_run(self):
    scheduler = self.mock_scheduler()
    helper = self.create_mux_helper(scheduler, self.create_query)
    tasks = helper.get_tasks(self.INSTANCES)

    scheduler.getTasksWithoutConfigs.assert_called_once_with(self.create_query(self.INSTANCES))
    assert 1 == len(tasks)

  def test_mux_run(self):
    expected_tasks = self.create_tasks()
    mux = self.mock_mux(expected_tasks)
    helper = self.create_mux_helper(None, self.create_query, scheduler_mux=mux)
    tasks = helper.get_tasks(self.INSTANCES)

    mux.enqueue_and_wait.assert_called_once_with(
        helper._get_tasks,
        self.INSTANCES,
        helper._create_aggregated_query)
    assert 1 == len(tasks)
