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
from twitter.common.contextutil import temporary_file

from apache.aurora.client.commands.core import cancel_update
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.common.aurora_job_key import AuroraJobKey

from gen.apache.aurora.api.ttypes import (
    Identity,
    JobKey,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery
)


class TestClientCancelUpdateCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.open_browser = False
    mock_options.shards = None
    mock_options.cluster = None
    mock_options.json = False
    mock_options.disable_all_hooks = False
    return mock_options

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
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_api_factory = self.setup_mock_api_factory()
    with contextlib.nested(
        patch('apache.aurora.client.commands.core.make_client_factory',
            return_value=mock_api_factory),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_make_client_factory, options, mock_get_job_config):
      mock_api = mock_api_factory.return_value

      cancel_update(['west/mchucarroll/test/hello'], mock_options)
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
    mock_options = self.setup_mock_options()

    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.releaseLock.return_value = self.get_release_lock_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_scheduler_proxy_class, mock_clusters, options, mock_get_job_config):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cancel_update(['west/mchucarroll/test/hello'], mock_options)

      # All that cancel_update really does is release the update lock.
      # So that's all we really need to check.
      assert mock_scheduler_proxy.releaseLock.call_count == 1
      assert mock_scheduler_proxy.releaseLock.call_args[0][0].key.job == JobKey(environment='test',
          role='mchucarroll', name='hello')
