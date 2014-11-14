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
import textwrap
import unittest

import pytest
from mock import create_autospec, Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import Context, EXIT_API_ERROR, EXIT_INVALID_CONFIGURATION, EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.client.cli.update import StartUpdate
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.config import AuroraConfig

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, mock_verb_options

from gen.apache.aurora.api.ttypes import (
    GetJobUpdateDetailsResult,
    GetJobUpdateSummariesResult,
    JobInstanceUpdateEvent,
    JobUpdate,
    JobUpdateAction,
    JobUpdateDetails,
    JobUpdateEvent,
    JobUpdateState,
    JobUpdateStatus,
    JobUpdateSummary,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    StartJobUpdateResult
)


class TestStartUpdateCommand(unittest.TestCase):

  def test_start_update_with_lock(self):
    command = StartUpdate()

    jobkey = AuroraJobKey("cluster", "role", "env", "job")
    mock_options = mock_verb_options(command)
    mock_options.instance_spec = TaskInstanceKey(jobkey, [])

    fake_context = FakeAuroraCommandContext()
    fake_context.set_options(mock_options)

    mock_config = create_autospec(spec=AuroraConfig, spec_set=True, instance=True)
    fake_context.get_job_config = Mock(return_value=mock_config)

    mock_api = fake_context.get_api("test")
    mock_api.start_job_update.return_value = AuroraClientCommandTest.create_blank_response(
      ResponseCode.LOCK_ERROR, "Error.")

    with pytest.raises(Context.CommandError):
      command.execute(fake_context)

    mock_api.start_job_update.assert_called_once_with(mock_config,
      mock_options.instance_spec.instance)
    assert fake_context.get_err()[0] == fake_context.LOCK_ERROR_MSG


class TestUpdateCommand(AuroraClientCommandTest):

  def test_start_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(updateId="id"))
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.start_job_update.return_value = resp
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['beta-update', 'start', self.TEST_JOBSPEC, fp.name])
        assert result == EXIT_OK

      update_url_msg = StartUpdate.UPDATE_MSG_TEMPLATE % (
          mock_context.get_update_page(mock_api, AuroraJobKey.from_path(self.TEST_JOBSPEC), "id"))

      assert mock_api.start_job_update.call_count == 1
      args, kwargs = mock_api.start_job_update.call_args
      assert isinstance(args[0], AuroraConfig)
      assert args[1] is None
      assert mock_context.get_out() == [update_url_msg]
      assert mock_context.get_err() == []

  def test_start_update_command_line_succeeds_noop_update(self):
    mock_context = FakeAuroraCommandContext()
    resp = self.create_simple_success_response()
    resp.details = [ResponseDetail(message="Noop update.")]
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api('west')
      mock_api.start_job_update.return_value = resp
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
      assert mock_context.get_out() == ["Noop update."]
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
      assert mock_context.get_out() == ["Update has been paused."]
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
      assert mock_context.get_out() == ["Update has been aborted."]
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
      assert mock_context.get_out() == ["Update has been resumed."]

  def test_update_invalid_config(self):
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
      assert mock_context.get_err() == ["Failed to resume update due to error:", "\tDamn"]

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
      assert mock_context.get_err() == ["Failed to abort update due to error:", "\tDamn"]

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
      assert mock_context.get_err() == ["Failed to pause update due to error:", "\tDamn"]

  @classmethod
  def get_status_query_response(cls):
    query_response = Response()
    query_response.responseCode = ResponseCode.OK
    query_response.result = Result()
    summaries = GetJobUpdateSummariesResult()
    query_response.result.getJobUpdateSummariesResult = summaries
    summaries.updateSummaries = [
        JobUpdateSummary(
            updateId="hello",
            jobKey=AuroraJobKey('west', 'mcc', 'test', 'hello'), user="me",
            state=JobUpdateState(status=JobUpdateStatus.ROLLING_FORWARD,
                createdTimestampMs=1411404927, lastModifiedTimestampMs=14114056030)),
        JobUpdateSummary(
            updateId="goodbye",
            jobKey=AuroraJobKey('west', 'mch', 'prod', 'goodbye'), user="me",
            state=JobUpdateState(status=JobUpdateStatus.ROLLING_BACK,
                createdTimestampMs=1411300632, lastModifiedTimestampMs=14114092632)),
        JobUpdateSummary(
            updateId="gasp",
            jobKey=AuroraJobKey('west', 'mcq', 'devel', 'gasp'), user="me",
            state=JobUpdateState(status=JobUpdateStatus.ROLL_FORWARD_PAUSED,
                createdTimestampMs=1411600891, lastModifiedTimestampMs=1411800891))]
    return query_response

  def test_list_updates_command(self):
    mock_context = FakeAuroraCommandContext()
    mock_context.get_api('west').query_job_updates.return_value = self.get_status_query_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "list", "west", "--user=me"])
      assert result == EXIT_OK
      assert mock_context.get_out_str() == textwrap.dedent("""\
          Job: west/mcc/test/hello, Id: hello, User: me, Status: ROLLING_FORWARD
            Created: 1411404927, Last Modified 14114056030
          Job: west/mch/prod/goodbye, Id: goodbye, User: me, Status: ROLLING_BACK
            Created: 1411300632, Last Modified 14114092632
          Job: west/mcq/devel/gasp, Id: gasp, User: me, Status: ROLL_FORWARD_PAUSED
            Created: 1411600891, Last Modified 1411800891""")

  def test_list_updates_command_json(self):
    mock_context = FakeAuroraCommandContext()
    mock_context.get_api('west').query_job_updates.return_value = self.get_status_query_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "list", "west", "--user=me", '--write-json'])
      assert result == EXIT_OK
      assert mock_context.get_out_str() == textwrap.dedent("""\
          [
            {
              "status": "ROLLING_FORWARD",
              "started": 1411404927,
              "lastModified": 14114056030,
              "user": "me",
              "jobkey": "west/mcc/test/hello",
              "id": "hello"
            },
            {
              "status": "ROLLING_BACK",
              "started": 1411300632,
              "lastModified": 14114092632,
              "user": "me",
              "jobkey": "west/mch/prod/goodbye",
              "id": "goodbye"
            },
            {
              "status": "ROLL_FORWARD_PAUSED",
              "started": 1411600891,
              "lastModified": 1411800891,
              "user": "me",
              "jobkey": "west/mcq/devel/gasp",
              "id": "gasp"
            }
          ]""")

  @classmethod
  def get_update_details_response(cls):
    query_response = Response()
    query_response.responseCode = ResponseCode.OK
    query_response.result = Result()
    details = JobUpdateDetails()
    query_response.result.getJobUpdateDetailsResult = GetJobUpdateDetailsResult(details=details)
    details.update = JobUpdate()
    details.update.summary = JobUpdateSummary(
        jobKey=AuroraJobKey('west', 'mcc', 'test', 'hello'),
        updateId="fake-update-identifier",
        user="me",
        state=JobUpdateState(status=JobUpdateStatus.ROLLING_FORWARD,
            createdTimestampMs=1411404927, lastModifiedTimestampMs=14114056030))
    details.updateEvents = [
      JobUpdateEvent(status=JobUpdateStatus.ROLLING_FORWARD,
         timestampMs=1411404927),
      JobUpdateEvent(status=JobUpdateStatus.ROLL_FORWARD_PAUSED,
         timestampMs=1411405000),
      JobUpdateEvent(status=JobUpdateStatus.ROLLING_FORWARD,
         timestampMs=1411405100)
    ]
    details.instanceEvents = [
      JobInstanceUpdateEvent(
          instanceId=1,
          timestampMs=1411404930,
          action=JobUpdateAction.INSTANCE_UPDATING),
      JobInstanceUpdateEvent(
          instanceId=2,
          timestampMs=1411404940,
          action=JobUpdateAction.INSTANCE_UPDATING),
      JobInstanceUpdateEvent(
          instanceId=1,
          timestampMs=1411404950,
          action=JobUpdateAction.INSTANCE_UPDATED),
      JobInstanceUpdateEvent(
          instanceId=2,
          timestampMs=1411404960,
          action=JobUpdateAction.INSTANCE_UPDATED)
    ]
    return query_response

  def test_update_status(self):
    mock_context = FakeAuroraCommandContext()
    api = mock_context.get_api('west')
    api.query_job_updates.return_value = self.get_status_query_response()
    api.get_job_update_details.return_value = self.get_update_details_response()

    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "status", "west/mcc/test/hello"])
      assert result == EXIT_OK
      assert mock_context.get_out() == [
          "Job: west/mcc/test/hello, UpdateID: fake-update-identifier",
          "Started YYYY-MM-DD HH:MM:SS, last updated: YYYY-MM-DD HH:MM:SS",
          "Current status: ROLLING_FORWARD",
          "Update events:",
          "  Status: ROLLING_FORWARD at YYYY-MM-DD HH:MM:SS",
          "  Status: ROLL_FORWARD_PAUSED at YYYY-MM-DD HH:MM:SS",
          "  Status: ROLLING_FORWARD at YYYY-MM-DD HH:MM:SS",
          "Instance events:",
          "  Instance 1 at YYYY-MM-DD HH:MM:SS: INSTANCE_UPDATING",
          "  Instance 2 at YYYY-MM-DD HH:MM:SS: INSTANCE_UPDATING",
          "  Instance 1 at YYYY-MM-DD HH:MM:SS: INSTANCE_UPDATED",
          "  Instance 2 at YYYY-MM-DD HH:MM:SS: INSTANCE_UPDATED"]
      mock_context.get_api("west").query_job_updates.assert_called_with(job_key=AuroraJobKey(
          'west', 'mcc', 'test', 'hello'))

  def test_update_status_json(self):
    mock_context = FakeAuroraCommandContext()
    api = mock_context.get_api('west')
    api.query_job_updates.return_value = self.get_status_query_response()
    api.get_job_update_details.return_value = self.get_update_details_response()

    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "status", "--write-json", "west/mcc/test/hello"])
      assert result == EXIT_OK
      mock_context.get_api("west").query_job_updates.assert_called_with(job_key=AuroraJobKey(
          'west', 'mcc', 'test', 'hello'))
      mock_context.get_api("west").get_job_update_details.assert_called_with('hello')
      assert mock_context.get_out_str() == textwrap.dedent("""\
        {
          "status": "ROLLING_FORWARD",
          "last_updated": 14114056030,
          "started": 1411404927,
          "update_events": [
            {
              "status": "ROLLING_FORWARD",
              "timestampMs": 1411404927
            },
            {
              "status": "ROLL_FORWARD_PAUSED",
              "timestampMs": 1411405000
            },
            {
              "status": "ROLLING_FORWARD",
              "timestampMs": 1411405100
            }
          ],
          "job": "west/mcc/test/hello",
          "updateId": "fake-update-identifier",
          "instance_update_events": [
            {
              "action": "INSTANCE_UPDATING",
              "instance": 1,
              "timestamp": 1411404930
            },
            {
              "action": "INSTANCE_UPDATING",
              "instance": 2,
              "timestamp": 1411404940
            },
            {
              "action": "INSTANCE_UPDATED",
              "instance": 1,
              "timestamp": 1411404950
            },
            {
              "action": "INSTANCE_UPDATED",
              "instance": 2,
              "timestamp": 1411404960
            }
          ]
        }""")
