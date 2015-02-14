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

import pytest
from mock import create_autospec, Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli import (
    Context,
    EXIT_API_ERROR,
    EXIT_INVALID_CONFIGURATION,
    EXIT_INVALID_PARAMETER,
    EXIT_OK
)
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.client.cli.update import StartUpdate, UpdateStatus
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


class TestStartUpdateCommand(AuroraClientCommandTest):

  def setUp(self):
    self._command = StartUpdate()
    self._job_key = AuroraJobKey("cluster", "role", "env", "job")
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.instance_spec = TaskInstanceKey(self._job_key, [])
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  @classmethod
  def create_mock_config(cls, is_cron=False):
    mock_config = create_autospec(spec=AuroraConfig, spec_set=True, instance=True)
    mock_raw_config = Mock()
    mock_raw_config.has_cron_schedule.return_value = is_cron
    mock_config.raw = Mock(return_value=mock_raw_config)
    return mock_config

  def test_start_update_with_lock(self):
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    self._mock_api.start_job_update.return_value = AuroraClientCommandTest.create_blank_response(
        ResponseCode.LOCK_ERROR,
        "Error.")

    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

    self._mock_api.start_job_update.assert_called_once_with(
        mock_config,
        self._mock_options.instance_spec.instance)

    self.assert_lock_message(self._fake_context)

  def test_update_cron_job_fails(self):
    mock_config = self.create_mock_config(is_cron=True)
    self._fake_context.get_job_config = Mock(return_value=mock_config)

    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

  def test_update_no_active_instance_check(self):
    self._mock_options.instance_spec = TaskInstanceKey(self.TEST_JOBKEY, [1])
    self._mock_options.strict = True

    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    self._mock_api.start_job_update.return_value = self.create_simple_success_response()

    self._command.execute(self._fake_context)

    self._mock_api.start_job_update.assert_called_once_with(
      mock_config,
      self._mock_options.instance_spec.instance)


class TestUpdateStatusCommand(AuroraClientCommandTest):

  def setUp(self):
    self._command = UpdateStatus()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  def test_status_fails_no_updates(self):
    response = self.create_simple_success_response()
    response.result = Result(
        getJobUpdateSummariesResult=GetJobUpdateSummariesResult(updateSummaries=[]))

    self._mock_api.query_job_updates.return_value = response

    assert EXIT_INVALID_PARAMETER == self._command.execute(self._fake_context)
    assert self._fake_context.get_err()[0] == "No updates found for job west/bozo/test/hello"


class TestUpdateCommand(AuroraClientCommandTest):

  def setUp(self):
    patcher = patch("time.ctime")
    self.addCleanup(patcher.stop)
    mock_ctime = patcher.start()
    mock_ctime.return_value = "YYYY-MM-DD HH:MM:SS"

  def test_start_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(updateId="id"))
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
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

  def get_status_query_response(self, count=3):
    query_response = Response()
    query_response.responseCode = ResponseCode.OK
    query_response.result = Result()
    summaries = GetJobUpdateSummariesResult()
    query_response.result.getJobUpdateSummariesResult = summaries
    summaries.updateSummaries = [JobUpdateSummary(
        updateId="%s" % i,
        jobKey=self.TEST_JOBKEY.to_thrift(),
        user="me",
        state=JobUpdateState(
            status=JobUpdateStatus.ROLLED_FORWARD,
            createdTimestampMs=1411404927,
            lastModifiedTimestampMs=14114056030)) for i in range(count)]
    return query_response

  def test_list_updates_command(self):
    mock_context = FakeAuroraCommandContext()
    mock_context.get_api('west').query_job_updates.return_value = self.get_status_query_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "list", self.TEST_CLUSTER, "--user=me"])
      assert result == EXIT_OK
      assert mock_context.get_out_str() == textwrap.dedent("""\
          Job: west/bozo/test/hello, Id: 0, User: me, Status: ROLLED_FORWARD
            Created: 1411404927, Last Modified 14114056030
          Job: west/bozo/test/hello, Id: 1, User: me, Status: ROLLED_FORWARD
            Created: 1411404927, Last Modified 14114056030
          Job: west/bozo/test/hello, Id: 2, User: me, Status: ROLLED_FORWARD
            Created: 1411404927, Last Modified 14114056030""")

  def test_list_updates_command_json(self):
    mock_context = FakeAuroraCommandContext()
    mock_context.get_api('west').query_job_updates.return_value = self.get_status_query_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "list", self.TEST_CLUSTER, "--user=me", '--write-json'])
      assert result == EXIT_OK
      assert mock_context.get_out_str() == textwrap.dedent("""\
          [
            {
              "status": "ROLLED_FORWARD",
              "started": 1411404927,
              "lastModified": 14114056030,
              "user": "me",
              "jobkey": "west/bozo/test/hello",
              "id": "0"
            },
            {
              "status": "ROLLED_FORWARD",
              "started": 1411404927,
              "lastModified": 14114056030,
              "user": "me",
              "jobkey": "west/bozo/test/hello",
              "id": "1"
            },
            {
              "status": "ROLLED_FORWARD",
              "started": 1411404927,
              "lastModified": 14114056030,
              "user": "me",
              "jobkey": "west/bozo/test/hello",
              "id": "2"
            }
          ]""")

  def get_update_details_response(self):
    query_response = Response()
    query_response.responseCode = ResponseCode.OK
    query_response.result = Result()
    details = JobUpdateDetails(
        update=JobUpdate(
            summary=JobUpdateSummary(
                jobKey=self.TEST_JOBKEY.to_thrift(),
                updateId="0",
                user="me",
                state=JobUpdateState(
                  status=JobUpdateStatus.ROLLING_FORWARD,
                  createdTimestampMs=1411404927,
                  lastModifiedTimestampMs=14114056030))),
        updateEvents=[
            JobUpdateEvent(
                status=JobUpdateStatus.ROLLING_FORWARD,
                timestampMs=1411404927),
            JobUpdateEvent(
                status=JobUpdateStatus.ROLL_FORWARD_PAUSED,
                timestampMs=1411405000),
            JobUpdateEvent(
                status=JobUpdateStatus.ROLLING_FORWARD,
                timestampMs=1411405100)],
        instanceEvents=[
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
                action=JobUpdateAction.INSTANCE_UPDATED)])
    query_response.result.getJobUpdateDetailsResult = GetJobUpdateDetailsResult(details=details)
    return query_response

  def test_update_status(self):
    mock_context = FakeAuroraCommandContext()
    api = mock_context.get_api(self.TEST_CLUSTER)
    api.query_job_updates.return_value = self.get_status_query_response(count=1)
    api.get_job_update_details.return_value = self.get_update_details_response()

    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "status", self.TEST_JOBSPEC])
      assert result == EXIT_OK
      assert mock_context.get_out() == [
          "Job: west/bozo/test/hello, UpdateID: 0",
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
      mock_context.get_api(self.TEST_CLUSTER).query_job_updates.assert_called_with(
          job_key=self.TEST_JOBKEY)

  def test_update_status_json(self):
    mock_context = FakeAuroraCommandContext()
    api = mock_context.get_api(self.TEST_CLUSTER)
    api.query_job_updates.return_value = self.get_status_query_response(count=1)
    api.get_job_update_details.return_value = self.get_update_details_response()

    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["beta-update", "status", "--write-json", self.TEST_JOBSPEC])
      assert result == EXIT_OK
      mock_context.get_api(self.TEST_CLUSTER).query_job_updates.assert_called_with(
          job_key=self.TEST_JOBKEY)
      mock_context.get_api(self.TEST_CLUSTER).get_job_update_details.assert_called_with("0")
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
          "job": "west/bozo/test/hello",
          "updateId": "0",
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
