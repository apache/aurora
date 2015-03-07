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

import pytest
from mock import create_autospec, Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import (
    Context,
    EXIT_COMMAND_FAILURE,
    EXIT_INTERRUPTED,
    EXIT_INVALID_CONFIGURATION,
    EXIT_OK,
    EXIT_UNKNOWN_ERROR
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.jobs import CreateJobCommand
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.config import AuroraConfig

from .util import (
    AuroraClientCommandTest,
    FakeAuroraCommandContext,
    FakeAuroraCommandLine,
    mock_verb_options
)

from gen.apache.aurora.api.ttypes import (
    JobKey,
    ResponseCode,
    Result,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery
)


class UnknownException(Exception):
  pass


class TestCreateJobCommand(AuroraClientCommandTest):

  def test_create_with_lock(self):
    command = CreateJobCommand()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")
    mock_options = mock_verb_options(command)
    mock_options.jobspec = jobkey
    mock_options.config_file = "/tmp/whatever"

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    mock_config = create_autospec(spec=AuroraConfig, spec_set=True, instance=True)
    mock_job_config = Mock()
    mock_job_config.has_cron_schedule.return_value = False
    mock_config.raw.return_value = mock_job_config
    fake_context.get_job_config = Mock(return_value=mock_config)
    mock_api = fake_context.get_api("test")

    mock_api.create_job.return_value = AuroraClientCommandTest.create_blank_response(
      ResponseCode.LOCK_ERROR, "Error.")

    with pytest.raises(Context.CommandError):
      command.execute(fake_context)

    mock_api.create_job.assert_called_once_with(mock_config)
    self.assert_lock_message(fake_context)


class TestClientCreateCommand(AuroraClientCommandTest):

  @classmethod
  def create_mock_status_query_result(cls, scheduleStatus):
    query_result = cls.create_simple_success_response()
    query_result.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=[
        cls.create_scheduled_task(0, initial_time=1000, status=scheduleStatus),
        cls.create_scheduled_task(1, initial_time=1004, status=scheduleStatus)
    ]))
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

  def test_simple_successful_create_job(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""

    # We'll patch out create_context, which will give us a fake context
    # object, and everything can be stubbed through that.
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        # TODO(maxim): Patching threading.Event with all possible namespace/patch/mock
        #              combinations did not produce the desired effect. Investigate why (AURORA-510)
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      # After making the client, create sets up a job monitor.
      # The monitor uses TaskQuery to get the tasks. It's called at least twice:once before
      # the job is created, and once after. So we need to set up mocks for the query results.
      mock_query = self.create_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.PENDING))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
                     fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 2)

  def test_simple_successful_create_job_open_page(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        # TODO(maxim): Patching threading.Event with all possible namespace/patch/mock
        #              combinations did not produce the desired effect. Investigate why (AURORA-510)
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_query = self.create_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.PENDING))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING', '--open-browser',
                     'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_OK

      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 2)
      assert mock_context.showed_urls == ["http://something_or_other/scheduler/bozo/test/hello"]

  def test_create_job_delayed(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_query = self.create_query()
      for result in [ScheduleStatus.PENDING, ScheduleStatus.PENDING, ScheduleStatus.RUNNING]:
        mock_context.add_expected_status_query_result(self.create_mock_status_query_result(result))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
            fp.name])
        self.assert_create_job_called(api)
        self.assert_scheduler_called(api, mock_query, 3)

  def test_create_job_failed(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      mock_context.add_expected_status_query_result(
          self.create_mock_status_query_result(ScheduleStatus.INIT))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_failed_createjob_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
            'west/bozo/test/hello', fp.name])
        assert result == EXIT_COMMAND_FAILURE

      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(api)

  def test_create_job_failed_invalid_config(self):
    """Run a test of the "create" command against a mocked-out API, with a configuration
    containing a syntax error"""
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_clause=oops'))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
            'west/bozo/test/hello', fp.name])
        assert result == EXIT_INVALID_CONFIGURATION

      # Now check that the right API calls got made.
      # Check that create_job was not called.
      api = mock_context.get_api('west')
      assert api.create_job.call_count == 0

  def test_interrupt(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.cli.jobs.Job.create_context',
            side_effect=KeyboardInterrupt())):
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_INTERRUPTED
        assert api.create_job.call_count == 0

  def test_interrupt_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.sleep'),
        patch('time.time', return_value=23),
        patch('apache.aurora.client.cli.jobs.Job.create_context',
            side_effect=Exception("Argh"))):
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_UNKNOWN_ERROR
        assert api.create_job.call_count == 0

  def test_simple_successful_create_job_output(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command generates the correct output.
    """
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.PENDING))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
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
        assert result == EXIT_OK
      assert mock_context.get_out() == [
          "Job create succeeded: job url=http://something_or_other/scheduler/bozo/test/hello"]
      assert mock_context.get_err() == []

  def test_create_job_startup_fails(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.PENDING))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))

      # We need to override the side_effect behavior of check_status in the context.
      def check_status_side_effect(*args):
        return self.create_error_response()

      mock_context.get_api("west").check_status.side_effect = check_status_side_effect

      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING', 'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_COMMAND_FAILURE
      assert mock_context.get_out() == []
      assert mock_context.get_err() == ["Error occurred while creating job west/bozo/test/hello"]

  def test_create_job_failed_output(self):
    """Test that a failed create generates the correct error messages"""
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      mock_context.add_expected_status_query_result(
          self.create_mock_status_query_result(ScheduleStatus.INIT))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_failed_createjob_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
            'west/bozo/test/hello', fp.name])
        assert result == EXIT_COMMAND_FAILURE

      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      assert mock_context.get_out() == []
      assert mock_context.get_err() == [
        'Job creation failed due to error:', '\tWhoops']

  def test_simple_successful_create_job_with_bindings(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""

    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_query = self.create_query()
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.PENDING))
      mock_context.add_expected_status_query_result(
        self.create_mock_status_query_result(ScheduleStatus.RUNNING))
      api = mock_context.get_api('west')
      api.create_job.return_value = self.get_createjob_response()

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_unbound_test_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'create', '--wait-until=RUNNING', '--bind', 'cluster_binding=west',
            '--bind', 'instances_binding=20', '--bind', 'TEST_BATCH=1',
            'west/bozo/test/hello',
            fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      self.assert_create_job_called(api)
      self.assert_scheduler_called(api, mock_query, 2)

  def test_failed_create_job_with_incomplete_bindings(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""

    with contextlib.nested(
        patch('threading._Event.wait')):

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_unbound_test_config())
        fp.flush()
        cmd = FakeAuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
            '--bind', 'cluster_binding=west',
           'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_INVALID_CONFIGURATION
        assert cmd.get_err() == [
            "Error executing command: Error loading configuration: "
            "TypeCheck(FAILED): MesosJob[update_config] failed: "
            "UpdateConfig[batch_size] failed: u'{{TEST_BATCH}}' not an integer"]

  def test_create_cron_job_fails(self):
    """Test a cron job is not accepted."""
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_valid_cron_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
            'west/bozo/test/hello', fp.name])
        assert result == EXIT_COMMAND_FAILURE
