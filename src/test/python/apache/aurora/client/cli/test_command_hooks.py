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

from mock import patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.command_hooks import CommandHook, GlobalCommandHookRegistry
from apache.aurora.config import AuroraConfig

from .util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.api.ttypes import (
    JobKey,
    Result,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery
)


class HookForTesting(CommandHook):
  def __init__(self, succeed):
    self.succeed = succeed
    self.ran_pre = False
    self.ran_post = False

  @property
  def name(self):
    return "test_hook"

  def get_nouns(self):
    return ["job"]

  def get_verbs(self, noun):
    if noun == "job":
      return ["create", "status"]
    else:
      return []

  def pre_command(self, noun, verb, context, commandline):
    self.ran_pre = True
    if self.succeed:
      return 0
    else:
      return 1

  def post_command(self, noun, verb, context, commandline, result):
    self.ran_post = True


class TestClientCreateCommand(AuroraClientCommandTest):

  @classmethod
  def create_mock_status_query_result(cls, scheduleStatus):
    query_result = cls.create_simple_success_response()
    if scheduleStatus == ScheduleStatus.INIT:
      # status query result for before job is launched.
      tasks = []
    else:
      task_one = cls.create_scheduled_task(0, initial_time=1000, status=scheduleStatus)
      task_two = cls.create_scheduled_task(1, initial_time=1004, status=scheduleStatus)
      tasks = [task_one, task_two]
    query_result.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    return query_result

  @classmethod
  def create_query(cls):
    return TaskQuery(
        jobKeys=[JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=cls.TEST_JOB)])

  @classmethod
  def get_createjob_response(cls):
    # Then, we call api.create_job(config)
    return cls.create_simple_success_response()

  @classmethod
  def assert_create_job_called(cls, mock_api):
    # Check that create_job was called exactly once, with an AuroraConfig parameter.
    assert mock_api.create_job.call_count == 1
    assert isinstance(mock_api.create_job.call_args_list[0][0][0], AuroraConfig)

  @classmethod
  def assert_scheduler_called(cls, mock_api, mock_query, num_queries):
    assert mock_api.scheduler_proxy.getTasksWithoutConfigs.call_count == num_queries
    mock_api.scheduler_proxy.getTasksWithoutConfigs.assert_called_with(mock_query, retry=False)

  def test_create_job_with_successful_hook(self):
    GlobalCommandHookRegistry.reset()
    command_hook = HookForTesting(True)
    GlobalCommandHookRegistry.register_command_hook(command_hook)
    mock_context = FakeAuroraCommandContext()
    with patch("apache.aurora.client.cli.jobs.Job.create_context", return_value=mock_context):
      mock_query = self.create_query()
      mock_context.add_expected_status_query_result(
          self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
          self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      mock_context.add_expected_status_query_result(
          self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      mock_context.get_api("west").check_status.side_effect = (
        lambda x: self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api("west")
      api.create_job.return_value = self.get_createjob_response()
      api.get_tier_configs.return_value = self.get_mock_tier_configurations()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(["job", "create", "--wait-until=RUNNING", "west/bozo/test/hello",
            fp.name])

      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 1)
      assert command_hook.ran_pre
      assert command_hook.ran_post

  def test_create_job_with_failed_hook(self):
    GlobalCommandHookRegistry.reset()
    command_hook = HookForTesting(False)
    GlobalCommandHookRegistry.register_command_hook(command_hook)
    mock_context = FakeAuroraCommandContext()
    with patch("apache.aurora.client.cli.jobs.Job.create_context", return_value=mock_context):
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api("west")
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(["job", "create", "--wait-until=RUNNING", "west/bozo/test/hello",
            fp.name])

        assert result == 1
        assert api.create_job.call_count == 0
        assert command_hook.ran_pre
        assert not command_hook.ran_post
