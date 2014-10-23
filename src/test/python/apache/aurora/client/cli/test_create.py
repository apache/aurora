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
import os
import shutil

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import (
    EXIT_COMMAND_FAILURE,
    EXIT_INTERRUPTED,
    EXIT_INVALID_CONFIGURATION,
    EXIT_OK,
    EXIT_UNKNOWN_ERROR
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    JobKey,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskEvent,
    TaskQuery
)


class UnknownException(Exception):
  pass


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
    mock_task_one = cls.create_mock_task('hello', 0, 1000, scheduleStatus)
    mock_task_two = cls.create_mock_task('hello', 1, 1004, scheduleStatus)
    mock_query_result.result.scheduleStatusResult.tasks = [mock_task_one, mock_task_two]
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
      mock_query = self.create_mock_query()
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

  def test_create_job_fail_and_write_log(self):
    """Check that when an unknown error occurs during command execution,
    the command-line framework catches it, and writes out an error log file
    containing the details of the error, including the command-line arguments
    passed to aurora to execute the command, and the stack trace of the error.
    """
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('time.time', return_value=23),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      api = mock_context.get_api('west')
      api.create_job.side_effect = UnknownException()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
           '--error-log-dir=./logged-errors', 'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_UNKNOWN_ERROR
        with open("./logged-errors/aurora-23.error-log", "r") as logfile:
          error_log = logfile.read()
          assert error_log.startswith("ERROR LOG: command arguments = %s" %
              ['job', 'create', '--wait-until=RUNNING', '--error-log-dir=./logged-errors',
               'west/bozo/test/hello',
              fp.name])
          assert "Traceback" in error_log
        if os.path.exists("./logged-errors"):
          shutil.rmtree("./logged-errors")

  def test_create_job_delayed(self):
    """Run a test of the "create" command against a mocked-out API:
    this time, make the monitor check status several times before successful completion.
    """
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_query = self.create_mock_query()
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
      assert api.scheduler_proxy.test_simple_successful_create_job.call_count == 0

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
            '--error-log-dir=./error-logs',
            fp.name])
        assert result == EXIT_UNKNOWN_ERROR
        assert api.create_job.call_count == 0
        if os.path.exists("./error-logs"):
          shutil.rmtree("./error-logs")

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
          "job create succeeded: job url=http://something_or_other/scheduler/bozo/test/hello"]
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
        'Job creation failed due to error:', '\tDamn']

  def test_simple_successful_create_job_with_bindings(self):
    """Run a test of the "create" command against a mocked-out API:
    Verifies that the creation command sends the right API RPCs, and performs the correct
    tests on the result."""

    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      mock_query = self.create_mock_query()
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

    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):

      # This is the real test: invoke create as if it had been called by the command line.
      with temporary_file() as fp:
        fp.write(self.get_unbound_test_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'create', '--wait-until=RUNNING',
            '--bind', 'cluster_binding=west',
           'west/bozo/test/hello',
            fp.name])
        assert result == EXIT_INVALID_CONFIGURATION
      assert mock_context.get_err() == [
          "TypeCheck(FAILED): MesosJob[update_config] failed: "
          "UpdateConfig[batch_size] failed: u'{{TEST_BATCH}}' not an integer"]
