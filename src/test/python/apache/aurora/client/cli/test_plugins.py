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

from apache.aurora.client.cli import ConfigurationPlugin
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.options import CommandOption
from apache.aurora.config import AuroraConfig

from .util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.api.ttypes import (
    JobKey,
    Result,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery
)


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


class TestPlugins(AuroraClientCommandTest):

  @classmethod
  def create_mock_status_query_result(cls, scheduleStatus):
    mock_query_result = cls.create_simple_success_response()
    if scheduleStatus == ScheduleStatus.INIT:
      # status query result for before job is launched.
      tasks = []
    else:
      mock_task_one = cls.create_scheduled_task(0, initial_time=1000, status=scheduleStatus)
      mock_task_two = cls.create_scheduled_task(1, initial_time=1004, status=scheduleStatus)
      tasks = [mock_task_one, mock_task_two]
    mock_query_result.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=tasks))
    return mock_query_result

  @classmethod
  def create_mock_query(cls):
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
    mock_api.scheduler_proxy.getTasksWithoutConfigs.assert_called_with(mock_query)

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
      self.assert_scheduler_called(api, mock_query, 1)
      # Check that the plugin did its job.
      assert mock_context.bogosity == "maximum"
      assert mock_context.after
