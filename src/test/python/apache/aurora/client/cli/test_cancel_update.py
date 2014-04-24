#
# Copyright 2013 Apache Software Foundation
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

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.common.aurora_job_key import AuroraJobKey
from twitter.common.contextutil import temporary_file

from gen.apache.aurora.api.ttypes import (
    Identity,
    JobKey,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery,
)

from mock import Mock, patch


class TestClientCancelUpdateCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_api_factory(cls):
    mock_api_factory, mock_api = cls.create_mock_api_factory()
    mock_api_factory.return_value.cancel_update.return_value = cls.get_cancel_update_response()
    return mock_api_factory

  @classmethod
  def create_mock_status_query_result(cls, scheduleStatus):
    mock_query_result = cls.create_simple_success_response()
    mock_query_result.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    if scheduleStatus == ScheduleStatus.INIT:
      # status query result for before job is launched.
      mock_query_result.result.scheduleStatusResult.tasks = []
    else:
      mock_task_one = cls.create_mock_task('hello', 0, 1000, scheduleStatus)
      mock_task_two = cls.create_mock_task('hello', 1, 1004, scheduleStatus)
      mock_query_result.result.scheduleStatusResult.tasks = [mock_task_one, mock_task_two]
    return mock_query_result

  @classmethod
  def create_mock_query(cls):
    return TaskQuery(owner=Identity(role=cls.TEST_ROLE), environment=cls.TEST_ENV,
        jobName=cls.TEST_JOB)

  @classmethod
  def get_cancel_update_response(cls):
    return cls.create_simple_success_response()

  @classmethod
  def assert_cancel_update_called(cls, mock_api):
    # Running cancel update should result in calling the API cancel_update
    # method once, with an AuroraJobKey parameter.
    assert mock_api.cancel_update.call_count == 1
    assert mock_api.cancel_update.called_with(
        AuroraJobKey(cls.TEST_CLUSTER, cls.TEST_ROLE, cls.TEST_ENV, cls.TEST_JOB),
        config=None)

  def test_simple_successful_cancel_update(self):
    """Run a test of the "kill" command against a mocked-out API:
    Verifies that the kill command sends the right API RPCs, and performs the correct
    tests on the result."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.cancel_update.return_value = self.create_simple_success_response()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'cancel-update', 'west/bozo/test/hello'])
      self.assert_cancel_update_called(mock_api)

  @classmethod
  def get_expected_task_query(cls, shards=None):
    instance_ids = frozenset(shards) if shards is not None else None
    # Helper to create the query that will be a parameter to job kill.
    return TaskQuery(taskIds=None, jobName=cls.TEST_JOB, environment=cls.TEST_ENV,
        instanceIds=instance_ids, owner=Identity(role=cls.TEST_ROLE, user=None))

  @classmethod
  def get_release_lock_response(cls):
    """Set up the response to a startUpdate API call."""
    return cls.create_simple_success_response()

  def test_cancel_update_api_level(self):
    """Test kill client-side API logic."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.releaseLock.return_value = self.get_release_lock_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'cancel-update', 'west/mchucarroll/test/hello'])

      # All that cancel_update really does is release the update lock.
      # So that's all we really need to check.
      assert mock_scheduler_proxy.releaseLock.call_count == 1
      assert mock_scheduler_proxy.releaseLock.call_args[0][0].key.job == JobKey(environment='test',
          role='mchucarroll', name='hello')
