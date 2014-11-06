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

from mock import patch

from apache.aurora.client.commands.core import status

from .util import AuroraClientCommandTest

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    JobKey,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)


class TestListJobs(AuroraClientCommandTest):
  @classmethod
  def create_mock_scheduled_tasks(cls):
    tasks = []
    for name in ['foo', 'bar', 'baz']:
      tasks.append(ScheduledTask(
          status=ScheduleStatus.RUNNING,
          failureCount=0,
          taskEvents=[TaskEvent(timestamp=123, status=ScheduleStatus.RUNNING, message='Hi there')],
          assignedTask=AssignedTask(
              instanceId=0,
              assignedPorts={},
              task=TaskConfig(
                  maxTaskFailures=1,
                  metadata={},
                  job=JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=name),
                  owner=Identity(role=cls.TEST_ROLE),
                  environment=cls.TEST_ENV,
                  jobName=name,
                  numCpus=2,
                  ramMb=2,
                  diskMb=2
              )
          )
      ))
    return tasks

  @classmethod
  def create_mock_scheduled_task_no_metadata(cls):
    result = cls.create_mock_scheduled_tasks()
    for job in result:
      job.assignedTask.task.metadata = None
    return result

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(tasks=set(cls.create_mock_scheduled_tasks())))
    return resp

  @classmethod
  def create_status_null_metadata(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(
            tasks=set(cls.create_mock_scheduled_task_no_metadata())))
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  def test_successful_status(self):
    """Test the status command."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    fake_options = {}
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=fake_options)):

      status(['west/mchucarroll/test/hello'], fake_options)
      # The status command sends a getTasksWithoutConfigs query to the scheduler,
      # and then prints the result.
      mock_scheduler_proxy.getTasksWithoutConfigs.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='mchucarroll', environment='test', name='hello')]))

  def test_unsuccessful_status(self):
    """Test the status command when the user asks the status of a job that doesn't exist."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_failed_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      self.assertRaises(SystemExit, status, ['west/mchucarroll/test/hello'], mock_options)
      mock_scheduler_proxy.getTasksWithoutConfigs.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='mchucarroll', environment='test', name='hello')]))

  def test_successful_status_nometadata(self):
    """Test the status command with no metadata."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_status_null_metadata()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)):

      status(['west/mchucarroll/test/hello'], mock_options)
      mock_scheduler_proxy.getTasksWithoutConfigs.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='mchucarroll', environment='test', name='hello')]))
