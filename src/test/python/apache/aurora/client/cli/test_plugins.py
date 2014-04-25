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

from apache.aurora.client.cli import (
    ConfigurationPlugin,
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_OK,
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.options import CommandOption
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.config import AuroraConfig
from mock import Mock, patch


class BogusPlugin(ConfigurationPlugin):
  """A test plugin, which uses all three plugin methods:
  - before_dispatch changes the command-line arguments to remove "bogus_bogus" if it's found.
    If it didn't work, we'll get an invalid parameter exception executing the test.
  - before_execution processes the "bogosity" argument.
  - after_execution sets a flag.
  """

  def get_options(self):
    return [CommandOption('--bogosity', type=str, help='Permitted bogosity level')]

  def before_dispatch(self, args):
    if args[0] == '--bogus_bogus':
       args = args[1:]
    return args


  def before_execution(self, context):
    context.bogosity = context.options.bogosity

  def after_execution(self, context, return_value):
    context.after = True
    raise self.Error("Oops")

class EmptyPlugin(ConfigurationPlugin):
  pass

class TestPlugins(AuroraClientCommandTest):

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
  def assert_create_job_called(cls, mock_api):
    # Check that create_job was called exactly once, with an AuroraConfig parameter.
    assert mock_api.create_job.call_count == 1
    assert isinstance(mock_api.create_job.call_args_list[0][0][0], AuroraConfig)

  @classmethod
  def assert_scheduler_called(cls, mock_api, mock_query, num_queries):
    assert mock_api.scheduler_proxy.getTasksStatus.call_count == num_queries
    mock_api.scheduler_proxy.getTasksStatus.assert_called_with(mock_query)

  def test_plugin_runs_in_create_job(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""

    # We'll patch out create_context, which will give us a fake context
    # object, and everything can be stubbed through that.
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      # After making the client, create sets up a job monitor.
      # The monitor uses TaskQuery to get the tasks. It's called at least twice:once before
      # the job is created, and once after. So we need to set up mocks for the query results.
      mock_query = self.create_mock_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.INIT))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.register_plugin(BogusPlugin())
        cmd.execute(['--bogus_bogus', 'job', 'create', '--bogosity=maximum', '--wait-until=RUNNING',
            'west/bozo/test/hello', fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 2)
      # Check that the plugin did its job.
      assert mock_context.bogosity == "maximum"
      assert mock_context.after == True

  def test_empty_plugins_in_create_job(self):
    """Installs a plugin that doesn't implement any of the plugin methods.
    Prior to AURORA-362, this would cause the client to crash with an empty
    argument list.
    """
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
        cmd.register_plugin(EmptyPlugin())
        cmd.execute(['job', 'create', '--wait-until=RUNNING',
            'west/bozo/test/hello', fp.name])
      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 2)

  def mock_print(self, str):
    for str in str.split('\n'):
      self.transcript.append(str)

  def mock_print_err(self, str):
    for str in str.split('\n'):
      self.err_transcript.append(str)


  def test_plugin_options_in_help(self):
    cmd = AuroraCommandLine()
    cmd.register_plugin(BogusPlugin())
    self.transcript = []
    self.err_transcript = []
    with patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
        side_effect=self.mock_print):
      assert cmd.execute(['help', 'job', 'status']) == EXIT_OK
      assert len(self.transcript) > 5
      assert self.transcript[0] == 'Usage for verb "job status":' in self.transcript
      assert not any('quota' in t for t in self.transcript)
      assert not any('list' in t for t in self.transcript)
      assert "Options:" in self.transcript
      assert any('bogosity' in t for t in self.transcript)

