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

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import (
    EXIT_API_ERROR,
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_CONFIGURATION,
    EXIT_OK
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.ttypes import JobKey


class TestCronNoun(AuroraClientCommandTest):

  def test_successful_schedule(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context)):

      api = mock_context.get_api('west')
      api.schedule_cron.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['cron', 'schedule', 'west/bozo/test/hello',
            fp.name])

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      assert api.schedule_cron.call_count == 1
      assert isinstance(api.schedule_cron.call_args[0][0], AuroraConfig)

  def test_schedule_failed(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      api = mock_context.get_api('west')
      api.schedule_cron.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_API_ERROR

      # Now check that the right API calls got made.
      # Check that create_job was called exactly once, with an AuroraConfig parameter.
      assert api.schedule_cron.call_count == 1

  def test_schedule_cron_failed_invalid_config(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.cron.CronNoun.create_context', return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_clause=oops'))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_INVALID_CONFIGURATION

      # Now check that the right API calls got made.
      # Check that create_job was not called.
      api = mock_context.get_api('west')
      assert api.schedule_cron.call_count == 0

  def test_schedule_cron_deep_api(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_scheduler_proxy.scheduleCronJob.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['cron', 'schedule', 'west/bozo/test/hello', fp.name])
        assert result == EXIT_OK
        assert mock_scheduler_proxy.scheduleCronJob.call_count == 1
        job = mock_scheduler_proxy.scheduleCronJob.call_args[0][0]
        assert job.key == JobKey("bozo", "test", "hello")

  def test_deschedule_cron_deep_api(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_scheduler_proxy.descheduleCronJob.return_value = self.create_simple_success_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'deschedule', 'west/bozo/test/hello'])
      assert result == EXIT_OK
      assert mock_scheduler_proxy.descheduleCronJob.call_count == 1
      mock_scheduler_proxy.descheduleCronJob.assert_called_with(JobKey(environment='test',
          role='bozo', name='hello'), None)

  def test_start_cron(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_scheduler_proxy.startCronJob.return_value = self.create_simple_success_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'start', 'west/bozo/test/hello'])
      assert result == EXIT_OK
      mock_scheduler_proxy.startCronJob.assert_called_once_with(JobKey("bozo", "test", "hello"))

  @classmethod
  def _create_getjobs_response(cls):
    response = cls.create_simple_success_response()
    response.result = Mock()
    response.result.getJobsResult = Mock()
    mockjob = Mock()
    mockjob.cronSchedule = "* * * * *"
    mockjob.key = Mock()
    mockjob.key.environment = "test"
    mockjob.key.name = "hello"
    mockjob.key.role = "bozo"
    response.result.getJobsResult.configs = [mockjob]
    return response

  def test_cron_status(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    with contextlib.nested(
        patch('time.sleep'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out')) as (
            _, _, _, mock_print):
      mock_scheduler_proxy.getJobs.return_value = self._create_getjobs_response()
      cmd = AuroraCommandLine()
      result = cmd.execute(['cron', 'show', 'west/bozo/test/hello'])

      assert result == EXIT_OK
      mock_scheduler_proxy.getJobs.assert_called_once_with("bozo")
      mock_print.assert_called_with("west/bozo/test/hello\t * * * * *")
