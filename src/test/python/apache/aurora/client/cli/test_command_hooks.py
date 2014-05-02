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

from twitter.common.contextutil import temporary_file

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery,
)

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.command_hooks import CommandHook, GlobalCommandHookRegistry
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig

from mock import Mock, patch


class HookForTesting(CommandHook):
  def __init__(self, succeed):
    self.succeed = succeed
    self.ran_pre = False
    self.ran_post = False

  def get_nouns(self):
    return ['job']

  def get_verbs(self, noun):
    if noun == 'job':
      return ['create', 'status']
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
  def create_mock_task(cls, task_id, instance_id, initial_time, status):
    mock_task = Mock(spec=ScheduledTask)
    mock_task.assignedTask = Mock(spec=AssignedTask)
    mock_task.assignedTask.taskId = task_id
    mock_task.assignedTask.instanceId = instance_id
    mock_task.status = status
    mock_task_event = Mock(spec=TaskEvent)
    mock_task_event.timestamp = initial_time
    mock_task.taskEvents = [mock_task_event]
    return mock_task

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
  def get_createjob_response(cls):
    # Then, we call api.create_job(config)
    return cls.create_simple_success_response()

  @classmethod
  def get_failed_createjob_response(cls):
    return cls.create_error_response()

  @classmethod
  def assert_create_job_called(cls, mock_api):
    # Check that create_job was called exactly once, with an AuroraConfig parameter.
    assert mock_api.create_job.call_count == 1
    assert isinstance(mock_api.create_job.call_args_list[0][0][0], AuroraConfig)

  @classmethod
  def assert_scheduler_called(cls, mock_api, mock_query, num_queries):
    assert mock_api.scheduler_proxy.getTasksStatus.call_count == num_queries
    mock_api.scheduler_proxy.getTasksStatus.assert_called_with(mock_query)

  def test_create_job_with_successful_hook(self):
    GlobalCommandHookRegistry.reset()
    command_hook = HookForTesting(True)
    GlobalCommandHookRegistry.register_command_hook(command_hook)
    self.generic_test_successful_hook(command_hook)

  def generic_test_successful_hook(self, command_hook):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      mock_query = self.create_mock_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
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
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
            fp.name])

        assert result == 1
        assert api.create_job.call_count == 0
        assert command_hook.ran_pre
        assert not command_hook.ran_post

  def test_load_dynamic_hooks(self):
    GlobalCommandHookRegistry.reset()
    hook_locals = GlobalCommandHookRegistry.load_project_hooks(
        './src/test/python/apache/aurora/client/cli')
    assert hook_locals['hooks'][0] in GlobalCommandHookRegistry.COMMAND_HOOKS

  def test_successful_dynamic_hook(self):
    GlobalCommandHookRegistry.reset()
    hook_locals = GlobalCommandHookRegistry.load_project_hooks(
        './src/test/python/apache/aurora/client/cli')
    self.generic_test_successful_hook(hook_locals['hooks'][0])

  def test_dynamic_hook_syntax_error(self):
    with patch('logging.warn') as log_patch:
      GlobalCommandHookRegistry.reset()
      hook_locals = GlobalCommandHookRegistry.load_project_hooks(
        './src/test/python/apache/aurora/client/cli/hook_test_data/bad_syntax')
      log_patch.assert_called_with('Error compiling hooks file '
          './src/test/python/apache/aurora/client/cli/hook_test_data/bad_syntax/AuroraHooks: '
          'invalid syntax (AuroraHooks, line 1)')

  def test_dynamic_hook_exec_error(self):
    with patch('logging.warn') as log_patch:
      GlobalCommandHookRegistry.reset()
      hook_locals = GlobalCommandHookRegistry.load_project_hooks(
        './src/test/python/apache/aurora/client/cli/hook_test_data/exec_error')
      log_patch.assert_called_with('Warning: error loading hooks file '
          './src/test/python/apache/aurora/client/cli/hook_test_data/exec_error/AuroraHooks: '
          'integer division or modulo by zero')

