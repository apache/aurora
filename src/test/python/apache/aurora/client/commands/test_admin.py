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

import contextlib

from mock import Mock, patch

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.commands.admin import query
from apache.aurora.client.commands.util import AuroraClientCommandTest

from gen.apache.aurora.api.constants import ACTIVE_STATES, TERMINAL_STATES
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    Response,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)


class TestAdminQueryCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls, force=False, shards=None, states='RUNNING', listformat=None):
    mock_options = Mock(spec=['force', 'shards', 'states', 'listformat'])
    mock_options.force = force
    mock_options.shards = shards
    mock_options.states = states
    mock_options.listformat = listformat or '%role%/%jobName%/%instanceId% %status%'
    mock_options.verbosity = False
    return mock_options

  @classmethod
  def create_response(cls, tasks, response_code=None):
    response_code = ResponseCode.OK if response_code is None else response_code
    resp = Response(responseCode=response_code, messageDEPRECATED='test')
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    return resp

  @classmethod
  def create_task(cls):
    return [ScheduledTask(
        assignedTask=AssignedTask(
            instanceId=0,
            task=TaskConfig(owner=Identity(role='test_role'), jobName='test_job')),
        status=ScheduleStatus.RUNNING
    )]

  @classmethod
  def task_query(cls):
    return TaskQuery(
        owner=Identity(role=None),
        environment=None,
        jobName=None,
        instanceIds=set(),
        statuses=set([ScheduleStatus.RUNNING]))

  def test_query(self):
    """Tests successful execution of the query command."""
    mock_options = self.setup_mock_options(force=True)
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS)):

      mock_scheduler_proxy.getTasksStatus.return_value = self.create_response(self.create_task())

      query([self.TEST_CLUSTER], mock_options)

      mock_scheduler_proxy.getTasksStatus.assert_called_with(self.task_query())

  def test_query_fails(self):
    """Tests failed execution of the query command."""
    mock_options = self.setup_mock_options()
    mock_api, mock_scheduler_proxy = self.create_mock_api()
    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS)):

      mock_scheduler_proxy.getTasksStatus.return_value = self.create_response(self.create_task())

      try:
        query([self.TEST_CLUSTER], mock_options)
      except SystemExit:
        pass
      else:
        assert 'Expected exception is not raised'
