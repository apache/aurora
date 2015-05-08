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

from mock import call, patch

from apache.aurora.client.cli.client import AuroraCommandLine

from .util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.api.ttypes import JobKey, Lock, LockKey, LockValidation, TaskQuery


class TestClientCancelUpdateCommand(AuroraClientCommandTest):
  def test_simple_successful_cancel_update(self):
    """Run a test of the "kill" command against a mocked-out API:
    Verifies that the kill command sends the right API RPCs, and performs the correct
    tests on the result."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.cancel_update.return_value = self.create_simple_success_response()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'cancel-update', self.TEST_JOBSPEC])
      assert mock_api.cancel_update.mock_calls == [call(self.TEST_JOBKEY, config=None)]

  @classmethod
  def get_expected_task_query(cls, shards=None):
    instance_ids = frozenset(shards) if shards is not None else None
    # Helper to create the query that will be a parameter to job kill.
    return TaskQuery(
        taskIds=None,
        instanceIds=instance_ids,
        jobKeys=[JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=cls.TEST_JOB)])

  @classmethod
  def get_release_lock_response(cls):
    """Set up the response to a startUpdate API call."""
    return cls.create_simple_success_response()

  def test_cancel_update_api_level(self):
    """Test kill client-side API logic."""
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.releaseLock.return_value = self.get_release_lock_response()
    with patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'cancel-update', self.TEST_JOBSPEC])

      # All that cancel_update really does is release the update lock.
      # So that's all we really need to check.
      assert mock_scheduler_proxy.releaseLock.mock_calls == [
          call(Lock(key=LockKey(job=self.TEST_JOBKEY.to_thrift())), LockValidation.UNCHECKED)]
