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

from mock import call, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.base import get_job_page
from apache.aurora.client.cli import (
    EXIT_API_ERROR,
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_OK
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.config import AuroraConfig

from .util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.api.ttypes import GetJobsResult, JobConfiguration, JobKey, Result


class TestCronNoun(AuroraClientCommandTest):

  def test_successful_schedule(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context)):

      api = mock_context.get_api('west')
      api.schedule_cron.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_cron_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['cron', 'schedule', self.TEST_JOBSPEC, fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      assert api.schedule_cron.call_count == 1
      assert isinstance(api.schedule_cron.call_args[0][0], AuroraConfig)

      # The last text printed out to the user should contain a url to the job
      assert get_job_page(api, self.TEST_JOBKEY) in mock_context.out[-1]

  def test_schedule_failed(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api('west')
      api.schedule_cron.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_cron_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_API_ERROR

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      assert api.schedule_cron.call_count == 1
      assert isinstance(api.schedule_cron.call_args[0][0], AuroraConfig)

  def test_schedule_failed_non_cron(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_COMMAND_FAILURE

  def test_schedule_cron_failed_invalid_config(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_invalid_cron_config('invalid_clause=oops'))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_INVALID_CONFIGURATION

      # Now check that the right API calls got made.
      # Check that create_job was not called.
      api = mock_context.get_api('west')
      assert api.schedule_cron.call_count == 0

  def test_schedule_cron_deep_api(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api("west")
      api.schedule_cron.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_cron_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_OK
        assert api.schedule_cron.call_count == 1
        config = api.schedule_cron.call_args[0][0]
        assert config.role() == "bozo"
        assert config.environment() == "test"
        assert config.name() == "hello"

  def test_deschedule_cron_deep_api(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api("west")
      api.deschedule_cron.return_value = self.create_simple_success_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'deschedule', self.TEST_JOBSPEC])
      assert result == EXIT_OK
      api.deschedule_cron.assert_called_once_with(self.TEST_JOBKEY)

  def test_start_cron(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api("west")
      api.start_cronjob.return_value = self.create_simple_success_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'start', 'west/bozo/test/hello'])
      assert result == EXIT_OK
      api.start_cronjob.assert_called_once_with(self.TEST_JOBKEY, config=None)

  def test_start_cron_open_browser(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api("west")
      api.start_cronjob.return_value = self.create_simple_success_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'start', self.TEST_JOBSPEC, '--open-browser'])
      assert result == EXIT_OK
      api.start_cronjob.assert_called_once_with(self.TEST_JOBKEY, config=None)
      assert self.mock_webbrowser.mock_calls == [
          call("http://something_or_other/scheduler/bozo/test/hello")
      ]

  @classmethod
  def _create_getjobs_response(cls):
    response = cls.create_simple_success_response()
    response.result = Result(getJobsResult=GetJobsResult(configs=[
        JobConfiguration(
            cronSchedule='* * * * *',
            key=JobKey(role='bozo', environment='test', name='hello'))
    ]))
    return response

  def test_cron_status(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api("west")
      api.get_jobs.return_value = self._create_getjobs_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'show', 'west/bozo/test/hello'])

      assert result == EXIT_OK
      api.get_jobs.assert_called_once_with("bozo")
      assert mock_context.get_out_str() == "west/bozo/test/hello\t * * * * *"

  def test_cron_status_multiple_jobs(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api("west")
      response = self.create_simple_success_response()
      response.result = Result(getJobsResult=GetJobsResult(configs=[
          JobConfiguration(
              key=JobKey(role='bozo', environment='test', name='hello'),
              cronSchedule='* * * * *'),
          JobConfiguration(
              key=JobKey(role='bozo', environment='test', name='hello2'),
              cronSchedule='* * * * *')
      ]))
      api.get_jobs.return_value = response

      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'show', 'west/bozo/test/hello'])

      assert result == EXIT_OK
      api.get_jobs.assert_called_once_with("bozo")
      assert mock_context.get_out_str() == "west/bozo/test/hello\t * * * * *"
