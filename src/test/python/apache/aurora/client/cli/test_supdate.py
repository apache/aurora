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
import json

import pytest
from mock import call, create_autospec, Mock, patch
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
from apache.aurora.client.cli.update import ListUpdates, StartUpdate, UpdateFilter, UpdateStatus
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.config import AuroraConfig

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, mock_verb_options

from gen.apache.aurora.api.constants import ACTIVE_JOB_UPDATE_STATES
from gen.apache.aurora.api.ttypes import (
    GetJobUpdateDetailsResult,
    GetJobUpdateSummariesResult,
    JobInstanceUpdateEvent,
    JobKey,
    JobUpdate,
    JobUpdateAction,
    JobUpdateDetails,
    JobUpdateEvent,
    JobUpdateKey,
    JobUpdateState,
    JobUpdateStatus,
    JobUpdateSummary,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    StartJobUpdateResult
)

UPDATE_KEY = JobUpdateKey(job=AuroraClientCommandTest.TEST_JOBKEY.to_thrift(), id="update_id")


def get_status_query_response(count=1):
  return Response(
      responseCode=ResponseCode.OK,
      result=Result(
          getJobUpdateSummariesResult=GetJobUpdateSummariesResult(
              updateSummaries=[
                  JobUpdateSummary(
                      key=UPDATE_KEY,
                      user="me",
                      state=JobUpdateState(
                          status=JobUpdateStatus.ROLLED_FORWARD,
                          createdTimestampMs=1411404927,
                          lastModifiedTimestampMs=14114056030)) for i in range(count)
              ]
          )
      )
  )


class TestStartUpdateCommand(AuroraClientCommandTest):

  def setUp(self):
    self._command = StartUpdate()
    self._job_key = AuroraJobKey.from_thrift("cluster", UPDATE_KEY.job)
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

    assert self._mock_api.start_job_update.mock_calls == [
        call(mock_config, None, self._mock_options.instance_spec.instance)
    ]

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

    assert self._mock_api.start_job_update.mock_calls == [
        call(mock_config, None, self._mock_options.instance_spec.instance)
    ]


class TestListUpdatesCommand(AuroraClientCommandTest):

  def setUp(self):
    self._command = ListUpdates()
    self._job_key = AuroraJobKey.from_thrift("cluster", UPDATE_KEY.job)
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.filter = UpdateFilter(
      cluster=self.TEST_CLUSTER, role=None, env=None, job=None)
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  def test_list_updates_command(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=3)
    self._mock_options.user = 'me'

    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._mock_api.query_job_updates.mock_calls == [
      call(role=None, user="me", job_key=None, update_statuses=None)
    ]

    # Ideally we would use a resource file for this, but i was unable to find a way to make that
    # work in both pants and pycharm.
    assert self._fake_context.get_out_str() == "\
JOB                                             UPDATE ID                            STATUS        \
  CREATED BY  STARTED             LAST MODIFIED      " + """
west/bozo/test/hello                            update_id                            ROLLED_FORWARD\
  me          1970-01-17T08:03:24 1970-06-13T08:34:16
west/bozo/test/hello                            update_id                            ROLLED_FORWARD\
  me          1970-01-17T08:03:24 1970-06-13T08:34:16
west/bozo/test/hello                            update_id                            ROLLED_FORWARD\
  me          1970-01-17T08:03:24 1970-06-13T08:34:16"""

  def test_list_updates_by_status(self):
    self._mock_options.status = ['paused', 'ROLLING_FORWARD']
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=3)
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._mock_api.query_job_updates.mock_calls == [
      call(
          role=None,
          user=None,
          job_key=None,
          update_statuses=set([
              JobUpdateStatus.ROLL_FORWARD_PAUSED,
              JobUpdateStatus.ROLL_BACK_PAUSED,
              JobUpdateStatus.ROLLING_FORWARD
          ]))
    ]

  def test_list_updates_by_env(self):
    self._mock_options.filter = UpdateFilter(
      cluster=self.TEST_CLUSTER, role='role', env='noenv', job=None)
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=3)
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._mock_api.query_job_updates.mock_calls == [
      call(role="role", user=None, job_key=None, update_statuses=None)
    ]
    # None of the returned values matched the env filter, so there is no output.
    assert self._fake_context.get_out_str() == ''

  def test_list_updates_command_json(self):
    self._mock_options.user = 'me'
    self._mock_options.write_json = True
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=3)
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert json.loads(self._fake_context.get_out_str()) == [
        {
            "status": "ROLLED_FORWARD",
            "started": "1970-01-17T08:03:24",
            "lastModified": "1970-06-13T08:34:16",
            "user": "me",
            "job": "west/bozo/test/hello",
            "id": "update_id"
        },
        {
            "status": "ROLLED_FORWARD",
            "started": "1970-01-17T08:03:24",
            "lastModified": "1970-06-13T08:34:16",
            "user": "me",
            "job": "west/bozo/test/hello",
            "id": "update_id"
        },
        {
            "status": "ROLLED_FORWARD",
            "started": "1970-01-17T08:03:24",
            "lastModified": "1970-06-13T08:34:16",
            "user": "me",
            "job": "west/bozo/test/hello",
            "id": "update_id"
        }
    ]


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

  CTIME = "$TIME"

  def setUp(self):
    patcher = patch("time.ctime")
    self.addCleanup(patcher.stop)
    self.mock_ctime = patcher.start()
    self.mock_ctime.return_value = self.CTIME

  def test_start_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(
      key=JobUpdateKey(job=JobKey(role="role", environment="env", name="name"), id="id")))
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
      mock_api.start_job_update.return_value = resp
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute([
            'update',
            'start',
            self.TEST_JOBSPEC,
            fp.name,
            '--message=hello'])
        assert result == EXIT_OK

      update_url_msg = StartUpdate.UPDATE_MSG_TEMPLATE % (
          'http://something_or_other/scheduler/role/env/name/id')

      assert mock_api.start_job_update.call_count == 1
      args, kwargs = mock_api.start_job_update.call_args
      assert isinstance(args[0], AuroraConfig)
      assert args[1] == 'hello'
      assert args[2] is None
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
        result = cmd.execute(['update', 'start', self.TEST_JOBSPEC, fp.name])
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
      mock_api.query_job_updates.return_value = get_status_query_response()
      mock_api.pause_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'pause', self.TEST_JOBSPEC, '-m=hello'])
        assert result == EXIT_OK

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.pause_job_update.mock_calls == [call(UPDATE_KEY, 'hello')]
      assert mock_context.get_out() == ["Update has been paused."]
      assert mock_context.get_err() == []

  def test_abort_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
      mock_api.query_job_updates.return_value = get_status_query_response()
      mock_api.abort_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'abort', self.TEST_JOBSPEC, '-m=hello'])
        assert result == EXIT_OK

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.abort_job_update.mock_calls == [call(UPDATE_KEY, 'hello')]
      assert mock_context.get_out() == ["Update has been aborted."]
      assert mock_context.get_err() == []

  def test_resume_update_command_line_succeeds(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
      mock_api.query_job_updates.return_value = get_status_query_response()
      mock_api.resume_job_update.return_value = self.create_simple_success_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'resume', self.TEST_JOBSPEC, '--message=hello'])
        assert result == EXIT_OK

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.resume_job_update.mock_calls == [call(UPDATE_KEY, 'hello')]
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
        result = cmd.execute(['update', 'start', self.TEST_JOBSPEC, fp.name])
        assert result == EXIT_INVALID_CONFIGURATION
        assert mock_api.start_job_update.mock_calls == []

  def test_resume_update_command_line_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
      mock_api.query_job_updates.return_value = get_status_query_response()
      mock_api.resume_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'resume', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.resume_job_update.mock_calls == [call(UPDATE_KEY, None)]
      assert mock_context.get_out() == []
      assert mock_context.get_err() == ["Failed to resume update due to error:", "\tWhoops"]

  def test_abort_update_command_line_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
      mock_api.query_job_updates.return_value = get_status_query_response()
      mock_api.abort_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'abort', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.abort_job_update.mock_calls == [call(UPDATE_KEY, None)]
      assert mock_context.get_out() == []
      assert mock_context.get_err() == ["Failed to abort update due to error:", "\tWhoops"]

  def test_abort_invalid_api_response(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)

      # Mimic the API returning two active updates for one job, which should be impossible.
      mock_api.query_job_updates.return_value = get_status_query_response(count=2)
      mock_api.abort_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'abort', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.abort_job_update.mock_calls == []
      assert mock_context.get_out() == []
      assert mock_context.get_err() == [
        'Error executing command: scheduler returned multiple active updates for this job.']

  def test_pause_update_command_line_error(self):
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_api = mock_context.get_api(self.TEST_CLUSTER)
      mock_api.query_job_updates.return_value = get_status_query_response()
      mock_api.pause_job_update.return_value = self.create_error_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['update', 'pause', self.TEST_JOBSPEC])
        assert result == EXIT_API_ERROR

      assert mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
      assert mock_api.pause_job_update.mock_calls == [call(UPDATE_KEY, None)]
      assert mock_context.get_out() == []
      assert mock_context.get_err() == ["Failed to pause update due to error:", "\tWhoops"]

  def get_update_details_response(self):
    query_response = Response()
    query_response.responseCode = ResponseCode.OK
    query_response.result = Result()
    details = JobUpdateDetails(
        update=JobUpdate(
            summary=JobUpdateSummary(
                key=UPDATE_KEY,
                user="me",
                state=JobUpdateState(
                  status=JobUpdateStatus.ROLLING_FORWARD,
                  createdTimestampMs=1000,
                  lastModifiedTimestampMs=2000))),
        updateEvents=[
            JobUpdateEvent(
                status=JobUpdateStatus.ROLLING_FORWARD,
                timestampMs=3000),
            JobUpdateEvent(
                status=JobUpdateStatus.ROLL_FORWARD_PAUSED,
                message="Investigating issues",
                timestampMs=4000),
            JobUpdateEvent(
                status=JobUpdateStatus.ROLLING_FORWARD,
                timestampMs=5000)],
        instanceEvents=[
            JobInstanceUpdateEvent(
                instanceId=1,
                timestampMs=6000,
                action=JobUpdateAction.INSTANCE_UPDATING),
            JobInstanceUpdateEvent(
                instanceId=2,
                timestampMs=7000,
                action=JobUpdateAction.INSTANCE_UPDATING),
            JobInstanceUpdateEvent(
                instanceId=1,
                timestampMs=8000,
                action=JobUpdateAction.INSTANCE_UPDATED),
            JobInstanceUpdateEvent(
                instanceId=2,
                timestampMs=9000,
                action=JobUpdateAction.INSTANCE_UPDATED)])
    query_response.result.getJobUpdateDetailsResult = GetJobUpdateDetailsResult(details=details)
    return query_response

  def test_update_status(self):
    mock_context = FakeAuroraCommandContext()
    api = mock_context.get_api(self.TEST_CLUSTER)
    api.query_job_updates.return_value = get_status_query_response(count=1)
    api.get_job_update_details.return_value = self.get_update_details_response()

    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["update", "status", self.TEST_JOBSPEC])
      assert result == EXIT_OK
      assert ('\n'.join(mock_context.get_out()) ==
          """Job: west/bozo/test/hello, UpdateID: update_id
Started %(ctime)s, last activity: %(ctime)s
Current status: ROLLING_FORWARD
Update events:
  Status: ROLLING_FORWARD at %(ctime)s
  Status: ROLL_FORWARD_PAUSED at %(ctime)s
      message: Investigating issues
  Status: ROLLING_FORWARD at %(ctime)s
Instance events:
  Instance 1 at %(ctime)s: INSTANCE_UPDATING
  Instance 2 at %(ctime)s: INSTANCE_UPDATING
  Instance 1 at %(ctime)s: INSTANCE_UPDATED
  Instance 2 at %(ctime)s: INSTANCE_UPDATED""" % {'ctime': self.CTIME})
      assert self.mock_ctime.mock_calls == [call(n) for n in range(1, 10)]
      assert mock_context.get_api(self.TEST_CLUSTER).query_job_updates.mock_calls == [
          call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)
      ]

  def test_update_status_json(self):
    mock_context = FakeAuroraCommandContext()
    api = mock_context.get_api(self.TEST_CLUSTER)
    update_status_response = get_status_query_response(count=1)
    api.query_job_updates.return_value = update_status_response
    api.get_job_update_details.return_value = self.get_update_details_response()

    with contextlib.nested(
        patch('apache.aurora.client.cli.update.Update.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      result = cmd.execute(["update", "status", "--write-json", self.TEST_JOBSPEC])
      assert result == EXIT_OK
      assert mock_context.get_api(self.TEST_CLUSTER).query_job_updates.mock_calls == [
          call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)
      ]
      assert mock_context.get_api(self.TEST_CLUSTER).get_job_update_details.mock_calls == [
          call(update_status_response.result.getJobUpdateSummariesResult.updateSummaries[0].key)
      ]
      assert json.loads(mock_context.get_out_str()) == {
          "status": "ROLLING_FORWARD",
          "last_updated": 2000,
          "started": 1000,
          "update_events": [
              {
                  "status": "ROLLING_FORWARD",
                  "timestampMs": 3000
              },
              {
                  "status": "ROLL_FORWARD_PAUSED",
                  "message": "Investigating issues",
                  "timestampMs": 4000
              },
              {
                  "status": "ROLLING_FORWARD",
                  "timestampMs": 5000
              }
          ],
          "job": "west/bozo/test/hello",
          "updateId": "update_id",
          "instance_update_events": [
              {
                  "action": "INSTANCE_UPDATING",
                  "instance": 1,
                  "timestamp": 6000
              },
              {
                  "action": "INSTANCE_UPDATING",
                  "instance": 2,
                  "timestamp": 7000
              },
              {
                  "action": "INSTANCE_UPDATED",
                  "instance": 1,
                  "timestamp": 8000
              },
              {
                  "action": "INSTANCE_UPDATED",
                  "instance": 2,
                  "timestamp": 9000
              }
          ]
      }
