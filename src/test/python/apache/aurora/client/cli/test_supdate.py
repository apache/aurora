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

from mock import patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import EXIT_API_ERROR, EXIT_INVALID_CONFIGURATION, EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from apache.aurora.config import AuroraConfig


class TestUpdateCommand(AuroraClientCommandTest):

  def test_start_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.start_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'start', self.TEST_JOBSPEC, fp.name])
        assert result == EXIT_OK

      assert mock_api.start_job_update.call_count == 1
      args, kwargs = mock_api.start_job_update.call_args
      assert isinstance(args[0], AuroraConfig)
      assert args[1] is None
      assert mock_context.get_out() == [
          "Scheduler-driven update of job west/bozo/test/hello has started."]
      assert mock_context.get_err() == []

  def test_pause_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.pause_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'pause', self.TEST_JOBSPEC])
        assert result == EXIT_OK

      mock_api.pause_job_update.assert_called_with(self.TEST_JOBKEY)
      assert mock_context.get_out() == [
          "Scheduler-driven update of job west/bozo/test/hello has been paused."]
      assert mock_context.get_err() == []

  def test_abort_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.abort_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'abort', self.TEST_JOBSPEC])
        assert result == EXIT_OK

      mock_api.abort_job_update.assert_called_with(self.TEST_JOBKEY)
      assert mock_context.get_out() == [
          "Scheduler-driven update of job west/bozo/test/hello has been aborted."]
      assert mock_context.get_err() == []

  def test_resume_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.resume_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'resume', self.TEST_JOBSPEC])
        assert result == EXIT_OK

      mock_api.resume_job_update.assert_called_with(self.TEST_JOBKEY)
      assert mock_context.get_out() == [
          "Scheduler-driven update of job west/bozo/test/hello has been resumed."]

  def test_update_invalid_config(self):
    return True
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_field=False,'))
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'start', self.TEST_JOBSPEC, fp.name])
        assert result == EXIT_INVALID_CONFIGURATION
        assert mock_api.start_job_update.call_count == 0

  def test_resume_update_command_line_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.resume_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'resume', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      mock_api.resume_job_update.assert_called_with(self.TEST_JOBKEY)
      assert mock_context.get_out() == []
      assert mock_context.get_err() == [
          "Error: Failed to resume scheduler-driven update; see log for details"]

  def test_abort_update_command_line_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.abort_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'abort', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      mock_api.abort_job_update.assert_called_with(self.TEST_JOBKEY)
      assert mock_context.get_out() == []
      assert mock_context.get_err() == [
          "Error: Failed to abort scheduler-driven update; see log for details"]

  def test_pause_update_command_line_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.pause_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'pause', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      mock_api.pause_job_update.assert_called_with(self.TEST_JOBKEY)
      assert mock_context.get_out() == []
      assert mock_context.get_err() == [
          "Error: Failed to pause scheduler-driven update; see log for details"]
