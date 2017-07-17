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

from mock import call, create_autospec

from apache.aurora.client.api.task_util import StatusHelper

from ...api_util import SchedulerProxyApiSpec

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Response,
    ResponseCode,
    ResponseDetail,
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
  def create_helper(cls, scheduler, query):
    return StatusHelper(scheduler, query)

  @classmethod
  def create_tasks(cls):
    return [ScheduledTask(assignedTask=AssignedTask(instanceId=index)) for index in cls.INSTANCES]

  @classmethod
  def mock_scheduler(cls, response_code=None):
    scheduler = create_autospec(spec=SchedulerProxyApiSpec, instance=True)
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, details=[ResponseDetail(message='test')])
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=cls.create_tasks()))
    scheduler.getTasksWithoutConfigs.return_value = resp
    return scheduler

  def test_run(self):
    scheduler = self.mock_scheduler()
    helper = self.create_helper(scheduler, self.create_query)
    tasks = helper.get_tasks(self.INSTANCES)

    assert scheduler.getTasksWithoutConfigs.mock_calls == [call(self.create_query(
      self.INSTANCES), retry=False)]
    assert 1 == len(tasks)
