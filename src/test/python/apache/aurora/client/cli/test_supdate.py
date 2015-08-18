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
import json
import time

import mock
import pytest
from mock import ANY, Mock, call, create_autospec
from pystachio import Empty

from apache.aurora.client.cli import (
    EXIT_API_ERROR,
    EXIT_COMMAND_FAILURE,
    EXIT_INVALID_PARAMETER,
    EXIT_OK,
    EXIT_UNKNOWN_ERROR,
    Context
)
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.client.cli.update import (
    AbortUpdate,
    ListUpdates,
    PauseUpdate,
    ResumeUpdate,
    StartUpdate,
    UpdateFilter,
    UpdateInfo,
    UpdateWait
)
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.config import AuroraConfig
from apache.aurora.config.schema.base import Job

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


def get_status_query_response(count=1, status=JobUpdateStatus.ROLLED_FORWARD):
  return Response(
      responseCode=ResponseCode.OK,
      result=Result(
          getJobUpdateSummariesResult=GetJobUpdateSummariesResult(
              updateSummaries=[
                  JobUpdateSummary(
                      key=UPDATE_KEY,
                      user="me",
                      state=JobUpdateState(
                          status=status,
                          createdTimestampMs=1411404927,
                          lastModifiedTimestampMs=14114056030)) for i in range(count)
              ]
          )
      )
  )


class TestStartUpdate(AuroraClientCommandTest):
  def setUp(self):
    self._command = StartUpdate()
    self._job_key = AuroraJobKey.from_thrift("cluster", UPDATE_KEY.job)
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.instance_spec = TaskInstanceKey(self._job_key, None)
    self._mock_options.wait = False
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  @classmethod
  def create_mock_config(cls, is_cron=False):
    # TODO(wfarner): Consider using a real AuroraConfig object for this.
    mock_config = create_autospec(spec=AuroraConfig, spec_set=True, instance=True)
    raw_config = Job(cron_schedule='something' if is_cron else Empty)
    mock_config.raw = Mock(return_value=raw_config)
    mock_config.cluster = Mock(return_value=cls.TEST_CLUSTER)
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

  def test_start_update_command_line_succeeds(self):
    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(
      key=JobUpdateKey(job=JobKey(role="role", environment="env", name="name"), id="id")))
    self._mock_api.start_job_update.return_value = resp
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    self._mock_options.instance_spec = TaskInstanceKey(self._job_key, None)
    self._mock_options.message = 'hello'
    assert self._command.execute(self._fake_context) == EXIT_OK

    assert self._mock_api.start_job_update.mock_calls == [
        call(ANY, 'hello', None)
    ]
    assert self._fake_context.get_out() == [
        StartUpdate.UPDATE_MSG_TEMPLATE %
        ('http://something_or_other/scheduler/role/env/name/update/id')
    ]
    assert self._fake_context.get_err() == []

  def test_start_update_and_wait_success(self):
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    self._mock_options.wait = True

    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(
        key=JobUpdateKey(job=JobKey(role="role", environment="env", name="name"), id="id")))
    self._mock_api.start_job_update.return_value = resp
    self._mock_api.query_job_updates.side_effect = [
        get_status_query_response(status=JobUpdateStatus.ROLLED_FORWARD)
    ]

    assert self._command.execute(self._fake_context) == EXIT_OK

    assert self._mock_api.start_job_update.mock_calls == [call(ANY, None, None)]
    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_key=resp.result.startJobUpdateResult.key)
    ]

    assert self._fake_context.get_out() == [
        StartUpdate.UPDATE_MSG_TEMPLATE %
        ('http://something_or_other/scheduler/role/env/name/update/id'),
        'Current state ROLLED_FORWARD'
    ]
    assert self._fake_context.get_err() == []

  def test_start_update_and_wait_rollback(self):
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    self._mock_options.wait = True

    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(
        key=JobUpdateKey(job=JobKey(role="role", environment="env", name="name"), id="id")))
    self._mock_api.start_job_update.return_value = resp
    self._mock_api.query_job_updates.side_effect = [
        get_status_query_response(status=JobUpdateStatus.ROLLED_BACK)
    ]

    assert self._command.execute(self._fake_context) == EXIT_COMMAND_FAILURE

  def test_start_update_and_wait_error(self):
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    self._mock_options.wait = True

    resp = self.create_simple_success_response()
    resp.result = Result(startJobUpdateResult=StartJobUpdateResult(
        key=JobUpdateKey(job=JobKey(role="role", environment="env", name="name"), id="id")))
    self._mock_api.start_job_update.return_value = resp
    self._mock_api.query_job_updates.side_effect = [
        get_status_query_response(status=JobUpdateStatus.ERROR)
    ]

    assert self._command.execute(self._fake_context) == EXIT_UNKNOWN_ERROR

  def test_start_update_command_line_succeeds_noop_update(self):
    resp = self.create_simple_success_response()
    resp.details = [ResponseDetail(message="Noop update.")]
    self._mock_api.start_job_update.return_value = resp
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)
    result = self._command.execute(self._fake_context)
    assert result == EXIT_OK

    assert self._mock_api.start_job_update.mock_calls == [call(ANY, None, None)]
    assert self._fake_context.get_out() == ["Noop update."]
    assert self._fake_context.get_err() == []

  def test_update_pulse_interval_too_small(self):
    mock_config = self.create_mock_config()
    self._fake_context.get_job_config = Mock(return_value=mock_config)

    error = Context.CommandError(100, 'something failed')
    self._mock_api.start_job_update.side_effect = error

    with pytest.raises(Context.CommandError) as e:
      self._command.execute(self._fake_context)

    assert e.value == error
    assert self._mock_api.start_job_update.mock_calls == [call(ANY, None, None)]


class TestListUpdates(AuroraClientCommandTest):

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
    self._mock_options.status = ListUpdates.STATUS_GROUPS.keys()
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=3)
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._mock_api.query_job_updates.mock_calls == [
        call(
            role=None,
            user=None,
            job_key=None,
            update_statuses=set(JobUpdateStatus._VALUES_TO_NAMES.keys()))
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
            "last_modified": "1970-06-13T08:34:16",
            "user": "me",
            "job": "west/bozo/test/hello",
            "id": "update_id"
        },
        {
            "status": "ROLLED_FORWARD",
            "started": "1970-01-17T08:03:24",
            "last_modified": "1970-06-13T08:34:16",
            "user": "me",
            "job": "west/bozo/test/hello",
            "id": "update_id"
        },
        {
            "status": "ROLLED_FORWARD",
            "started": "1970-01-17T08:03:24",
            "last_modified": "1970-06-13T08:34:16",
            "user": "me",
            "job": "west/bozo/test/hello",
            "id": "update_id"
        }
    ]


class TestUpdateStatus(AuroraClientCommandTest):
  def setUp(self):
    self._command = UpdateInfo()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._mock_options.id = None
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  def test_status_fails_no_updates(self):
    response = self.create_simple_success_response()
    response.result = Result(
        getJobUpdateSummariesResult=GetJobUpdateSummariesResult(updateSummaries=[]))

    self._mock_api.query_job_updates.return_value = response

    assert EXIT_INVALID_PARAMETER == self._command.execute(self._fake_context)
    assert self._fake_context.get_err() == ["There is no active update for this job."]


class TestPauseUpdate(AuroraClientCommandTest):
  def setUp(self):
    self._command = PauseUpdate()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  def test_pause_update_command_line_succeeds(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response()
    self._mock_api.pause_job_update.return_value = self.create_simple_success_response()
    self._mock_options.message = 'hello'
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.pause_job_update.mock_calls == [call(UPDATE_KEY, 'hello')]
    assert self._fake_context.get_out() == ["Update has been paused."]
    assert self._fake_context.get_err() == []

  def test_pause_update_command_line_error(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response()
    self._mock_api.pause_job_update.return_value = self.create_error_response()
    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)

    assert self._mock_api.query_job_updates.mock_calls == [
      call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.pause_job_update.mock_calls == [call(UPDATE_KEY, None)]
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == ["Failed to pause update due to error:", "\tWhoops"]


class TestAbortUpdate(AuroraClientCommandTest):
  def setUp(self):
    self._command = AbortUpdate()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  def test_abort_update_command_line_succeeds(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response()
    self._mock_api.abort_job_update.return_value = self.create_simple_success_response()
    self._mock_options.message = 'hello'
    assert self._command.execute(self._fake_context) == EXIT_OK

    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.abort_job_update.mock_calls == [call(UPDATE_KEY, 'hello')]
    assert self._fake_context.get_out() == ["Update has been aborted."]
    assert self._fake_context.get_err() == []

  def test_abort_update_command_line_error(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response()
    self._mock_api.abort_job_update.return_value = self.create_error_response()

    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)
    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.abort_job_update.mock_calls == [call(UPDATE_KEY, None)]
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == ["Failed to abort update due to error:", "\tWhoops"]

  def test_abort_invalid_api_response(self):
    # Mimic the API returning two active updates for one job, which should be impossible.
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=2)
    self._mock_api.abort_job_update.return_value = self.create_error_response()
    with pytest.raises(Context.CommandError) as error:
      self._command.execute(self._fake_context)
      assert error.message == (
        'scheduler returned multiple active updates for this job.')

    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.abort_job_update.mock_calls == []
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == []


class TestResumeUpdate(AuroraClientCommandTest):
  def setUp(self):
    self._command = ResumeUpdate()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  def test_resume_update_command_line_succeeds(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response()
    self._mock_api.resume_job_update.return_value = self.create_simple_success_response()
    self._mock_options.message = 'hello'
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.resume_job_update.mock_calls == [call(UPDATE_KEY, 'hello')]
    assert self._fake_context.get_out() == ["Update has been resumed."]
    assert self._fake_context.get_err() == []

  def test_resume_update_command_line_error(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response()
    self._mock_api.resume_job_update.return_value = self.create_error_response()
    with pytest.raises(Context.CommandError):
      self._command.execute(self._fake_context)
    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)]
    assert self._mock_api.resume_job_update.mock_calls == [call(UPDATE_KEY, None)]
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == ["Failed to resume update due to error:", "\tWhoops"]


class TestUpdateInfo(AuroraClientCommandTest):
  def setUp(self):
    self._command = UpdateInfo()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._mock_options.id = None
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')

  @classmethod
  def get_update_details_response(cls):
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

  def test_active_update_info(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=1)
    self._mock_api.get_job_update_details.return_value = self.get_update_details_response()
    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._fake_context.get_api(self.TEST_CLUSTER).query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)
    ]
    assert '\n'.join(self._fake_context.get_out()) == """\
Job: west/bozo/test/hello, UpdateID: update_id
Started 1970-01-01T00:00:01, last activity: 1970-01-01T00:00:02
Current status: ROLLING_FORWARD
Update events:
  Status: ROLLING_FORWARD at 1970-01-01T00:00:03
  Status: ROLL_FORWARD_PAUSED at 1970-01-01T00:00:04
      message: Investigating issues
  Status: ROLLING_FORWARD at 1970-01-01T00:00:05
Instance events:
  Instance 1 at 1970-01-01T00:00:06: INSTANCE_UPDATING
  Instance 2 at 1970-01-01T00:00:07: INSTANCE_UPDATING
  Instance 1 at 1970-01-01T00:00:08: INSTANCE_UPDATED
  Instance 2 at 1970-01-01T00:00:09: INSTANCE_UPDATED"""

  def test_update_info(self):
    self._mock_options.id = 'update_id'
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=1)
    self._mock_api.get_job_update_details.return_value = self.get_update_details_response()
    assert self._command.execute(self._fake_context) == EXIT_OK

  def test_update_info_json(self):
    self._mock_options.write_json = True
    update_status_response = get_status_query_response(count=1)
    self._mock_api.query_job_updates.return_value = update_status_response
    self._mock_api.get_job_update_details.return_value = self.get_update_details_response()
    assert self._command.execute(self._fake_context) == EXIT_OK

    assert self._mock_api.query_job_updates.mock_calls == [
        call(update_statuses=ACTIVE_JOB_UPDATE_STATES, job_key=self.TEST_JOBKEY)
    ]
    assert self._mock_api.get_job_update_details.mock_calls == [
        call(update_status_response.result.getJobUpdateSummariesResult.updateSummaries[0].key)
    ]
    assert json.loads(self._fake_context.get_out_str()) == {
        "status": "ROLLING_FORWARD",
        "last_modified": "1970-01-01T00:00:02",
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


class TestUpdateWait(AuroraClientCommandTest):
  def setUp(self):
    self._command = UpdateWait(clock=mock.create_autospec(spec=time))
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.jobspec = self.TEST_JOBKEY
    self._mock_options.id = 'update_id'
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api('UNUSED')
    self._fetch_call = call(
        update_key=JobUpdateKey(job=self.TEST_JOBKEY.to_thrift(), id=self._mock_options.id))

  def test_wait_success(self):
    updating_response = get_status_query_response(status=JobUpdateStatus.ROLLING_FORWARD)
    updated_response = get_status_query_response(status=JobUpdateStatus.ROLLED_FORWARD)

    self._mock_api.query_job_updates.side_effect = [updating_response, updated_response]

    assert self._command.execute(self._fake_context) == EXIT_OK
    assert self._fake_context.get_out() == ['Current state ROLLING_FORWARD',
                                            'Current state ROLLED_FORWARD']

    assert self._mock_api.query_job_updates.mock_calls == [self._fetch_call, self._fetch_call]

  def test_wait_rolled_back(self):
    response = get_status_query_response(status=JobUpdateStatus.ROLLED_BACK)

    self._mock_api.query_job_updates.side_effect = [response]

    assert self._command.execute(self._fake_context) == EXIT_COMMAND_FAILURE

  def test_wait_non_ok_response(self):
    self._mock_api.query_job_updates.return_value = Response(
        responseCode=ResponseCode.INVALID_REQUEST)

    with pytest.raises(Context.CommandError) as e:
      self._command.execute(self._fake_context)
    assert e.value.code == EXIT_API_ERROR

    assert self._fake_context.get_out() == []
    assert self._mock_api.query_job_updates.mock_calls == [self._fetch_call]

  def test_update_wait_not_found(self):
    self._mock_api.query_job_updates.return_value = Response(
        responseCode=ResponseCode.OK,
        result=Result(getJobUpdateSummariesResult=GetJobUpdateSummariesResult(updateSummaries=[])))

    with pytest.raises(Context.CommandError) as e:
      self._command.execute(self._fake_context)
    assert e.value.code == EXIT_INVALID_PARAMETER

    assert self._fake_context.get_out() == []
    assert self._mock_api.query_job_updates.mock_calls == [self._fetch_call]

  def test_wait_scheduler_returns_multiple_summaries(self):
    self._mock_api.query_job_updates.return_value = get_status_query_response(count=2)

    with pytest.raises(Context.CommandError) as e:
      self._command.execute(self._fake_context)
    assert e.value.code == EXIT_API_ERROR

    assert self._fake_context.get_out() == []
    assert self._mock_api.query_job_updates.mock_calls == [self._fetch_call]
