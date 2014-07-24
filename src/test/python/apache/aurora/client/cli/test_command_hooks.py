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

from apache.aurora.client.cli import EXIT_PERMISSION_VIOLATION
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.command_hooks import CommandHook, GlobalCommandHookRegistry
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
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


class SecondHookForTesting(CommandHook):
  def __init__(self, succeed):
    self.succeed = succeed
    self.ran_pre = False
    self.ran_post = False

  @property
  def name(self):
    return "second"

  def get_nouns(self):
    return ["job"]

  def get_verbs(self, noun):
    if noun == "job":
      return ["kill", "killall", "create"]
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
      mock_task_one = cls.create_mock_task("hello", 0, 1000, scheduleStatus)
      mock_task_two = cls.create_mock_task("hello", 1, 1004, scheduleStatus)
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
    assert mock_api.scheduler_proxy.getTasksWithoutConfigs.call_count == num_queries
    mock_api.scheduler_proxy.getTasksWithoutConfigs.assert_called_with(mock_query)

  def test_create_job_with_successful_hook(self):
    GlobalCommandHookRegistry.reset()
    command_hook = HookForTesting(True)
    GlobalCommandHookRegistry.register_command_hook(command_hook)
    self.generic_test_successful_hook(command_hook)

  def generic_test_successful_hook(self, command_hook):
    mock_context = FakeAuroraCommandContext()
    with patch("apache.aurora.client.cli.jobs.Job.create_context", return_value=mock_context):
      mock_query = self.create_mock_query()
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

  def test_load_dynamic_hooks(self):
    GlobalCommandHookRegistry.reset()
    hook_locals = GlobalCommandHookRegistry.load_project_hooks(
        "./src/test/python/apache/aurora/client/cli")
    assert hook_locals["hooks"][0] in GlobalCommandHookRegistry.COMMAND_HOOKS

  def test_successful_dynamic_hook(self):
    GlobalCommandHookRegistry.reset()
    hook_locals = GlobalCommandHookRegistry.load_project_hooks(
        "./src/test/python/apache/aurora/client/cli")
    self.generic_test_successful_hook(hook_locals["hooks"][0])

  def test_dynamic_hook_syntax_error(self):
    with patch("logging.warn") as log_patch:
      GlobalCommandHookRegistry.reset()
      GlobalCommandHookRegistry.load_project_hooks(
          "./src/test/python/apache/aurora/client/cli/hook_test_data/bad_syntax")
      log_patch.assert_called_with("Error compiling hooks file "
          "./src/test/python/apache/aurora/client/cli/hook_test_data/bad_syntax/AuroraHooks: "
          "invalid syntax (AuroraHooks, line 15)")

  def test_dynamic_hook_exec_error(self):
    with patch("logging.warn") as log_patch:
      GlobalCommandHookRegistry.reset()
      GlobalCommandHookRegistry.load_project_hooks(
          "./src/test/python/apache/aurora/client/cli/hook_test_data/exec_error")
      log_patch.assert_called_with("Warning: error loading hooks file "
          "./src/test/python/apache/aurora/client/cli/hook_test_data/exec_error/AuroraHooks: "
          "integer division or modulo by zero")

  def assert_skip_allowed(self, context, skip_opt, user, noun, verb, args):
    """Checks that a hook would be allowed to be skipped in a command invocation"""
    GlobalCommandHookRegistry.get_required_hooks(context, skip_opt, noun, verb, user)

  def assert_skip_forbidden(self, context, skip_opt, user, noun, verb, args):
    """Checks that a hook would NOT be allowed to be skipped in a command invocation"""
    try:
      # assertRaises doesn't work with classmethods.
      context.args = args
      GlobalCommandHookRegistry.get_required_hooks(context, skip_opt, noun, verb, user)
      self.fail("Should have thrown an error.")
    except context.CommandError:
      pass

  def test_json_skip_rules(self):
    """Load up a set of skips, specified in JSON, and then check a bunch of different
    cases to see that the skip rules work correctly.
    """
    mock_response = Mock()
    mock_context = FakeAuroraCommandContext()
    with patch("requests.get", return_value=mock_response):
      mock_response.json.return_value = {
        "a": {
          "users": ["bozo", "clown"],
          "commands": {"job": ["killall"]},
          "hooks": ["test_hook", "second"]
        },
        "b": {
          "commands": {"user": ["kick"]},
        }
      }
      GlobalCommandHookRegistry.reset()
      GlobalCommandHookRegistry.setup("http://foo.bar")
      command_hook_one = HookForTesting(False)
      GlobalCommandHookRegistry.register_command_hook(command_hook_one)
      command_hook_two = SecondHookForTesting(True)
      GlobalCommandHookRegistry.register_command_hook(command_hook_two)
      assert len(GlobalCommandHookRegistry.SKIP_HOOK_RULES) == 2
      # Should not be allowed: no skip rule permitting skipping hooks on job kill
      self.assert_skip_forbidden(mock_context, "all", "beezus", "job", "kill", ["a", "b", "c"])
      # Should not be allowed: there are hooks on "job killall", and beezus doesn't satisfy
      # their exception rules
      self.assert_skip_forbidden(mock_context, "all", "beezus", "job", "kill", ["a", "b", "c"])
      # Should be allowed: there's a rule allowing bozo to skip test_hook on job killall.
      self.assert_skip_allowed(mock_context, "test_hook", "bozo", "job", "killall", ["a", "b", "c"])
      # Should be allowed: since there's only one hook on killall, this is equivalent to
      # the previous.
      self.assert_skip_allowed(mock_context, "all", "bozo", "job", "killall", ["a", "b", "c"])
      # Should be allowed: there's a rule allowing anyone to skip any hook on "user kick".
      self.assert_skip_allowed(mock_context, "test_hook,something_else", "nobody", "user",
          "kick", ["a", "b", "c"])
      # should be allowed: there are no hooks in place for "user kick", so all is acceptable.
      self.assert_skip_allowed(mock_context, "all", "nobody", "user", "kick", ["a", "b", "c"])

  def test_skip_hooks_in_create(self):
    """Run a test of create, with a hook that should forbid running create, but
    with a user who's covered by one of the hook skip exception rules - so it
    should succeed.
    """

    GlobalCommandHookRegistry.reset()
    command_hook = HookForTesting(True)
    GlobalCommandHookRegistry.register_command_hook(command_hook)
    command_hook_two = SecondHookForTesting(False)
    GlobalCommandHookRegistry.register_command_hook(command_hook_two)
    mock_response = Mock()
    mock_context = FakeAuroraCommandContext()

    with contextlib.nested(
        patch("apache.aurora.client.cli.jobs.Job.create_context", return_value=mock_context),
        patch("requests.get", return_value=mock_response),
        patch("getpass.getuser", return_value="bozo")):
      mock_response.json.return_value = {
        "a": {
          "users": ["bozo", "clown"],
          "commands": {"job": ["killall", "create"]},
          "hooks": ["test_hook", "second"]
        },
        "b": {
          "commands": {"user": ["kick"]},
        }
      }

      mock_query = self.create_mock_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api("west")
      api.create_job.return_value = self.get_createjob_response()
      GlobalCommandHookRegistry.setup("http://foo.bar")

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(["job", "create", "--skip-hooks=second", "--wait-until=RUNNING",
            "west/bozo/test/hello", fp.name])
        assert result == 0
        self.assert_create_job_called(api)
        self.assert_scheduler_called(api, mock_query, 1)
        assert command_hook.ran_pre
        assert command_hook.ran_post

  def test_cannot_skip_hooks_in_create(self):
    """This time, the hook shouldn't be skippable, because we use a username
    who isn't allowed by the hook exception rule.
    """
    GlobalCommandHookRegistry.reset()
    command_hook = HookForTesting(True)
    GlobalCommandHookRegistry.register_command_hook(command_hook)
    command_hook_two = SecondHookForTesting(False)
    GlobalCommandHookRegistry.register_command_hook(command_hook_two)
    mock_response = Mock()
    mock_context = FakeAuroraCommandContext()

    with contextlib.nested(
        patch("apache.aurora.client.cli.jobs.Job.create_context", return_value=mock_context),
        patch("requests.get", return_value=mock_response),
        patch("getpass.getuser", return_value="beezus")):
      mock_response.json.return_value = {
        "a": {
          "users": ["bozo", "clown"],
          "commands": {"job": ["killall", "create"]},
          "hooks": ["test_hook", "second"]
        },
        "b": {
          "commands": {"user": ["kick"]},
        }
      }

      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api("west")
      api.create_job.return_value = self.get_createjob_response()
      GlobalCommandHookRegistry.setup("http://foo.bar")

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(["job", "create", "--skip-hooks=second", "--wait-until=RUNNING",
            "west/bozo/test/hello", fp.name])
        # Check that it returns the right error code, and that create_job didn't get called.
        assert result == EXIT_PERMISSION_VIOLATION
        assert api.create_job.call_count == 0
