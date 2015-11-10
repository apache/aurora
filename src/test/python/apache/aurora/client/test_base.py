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
import unittest

import mock

from apache.aurora.client import base
from apache.aurora.common.pex_version import UnknownVersion

from gen.apache.aurora.api.ttypes import (
    PopulateJobResult,
    Response,
    ResponseCode,
    ResponseDetail,
    Result,
    TaskConfig
)


class TestBase(unittest.TestCase):

  def test_format_response_with_message(self):
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    formatted = base.format_response(resp)
    assert formatted == 'Response from scheduler: ERROR (message: Error)'

  def test_format_response_with_details(self):
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    formatted = base.format_response(resp)
    assert formatted == 'Response from scheduler: ERROR (message: Error)'

  def test_combine_messages(self):
    resp = Response(responseCode=ResponseCode.ERROR)
    assert base.combine_messages(resp) == ''
    resp = Response(responseCode=ResponseCode.ERROR, details=[])
    assert base.combine_messages(resp) == ''
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail(message='Error')])
    assert base.combine_messages(resp) == 'Error'
    resp = Response(responseCode=ResponseCode.ERROR, details=[ResponseDetail()])
    assert base.combine_messages(resp) == 'Unknown error'
    resp = Response(
        responseCode=ResponseCode.ERROR,
        details=[ResponseDetail(message='Error1'), ResponseDetail(message='Error2')])
    assert base.combine_messages(resp) == 'Error1, Error2'

  def test_get_populated_task_config_set(self):
    config = TaskConfig()
    resp = Response(responseCode=ResponseCode.OK, result=Result(populateJobResult=PopulateJobResult(
        taskConfig=config)))
    assert config == resp.result.populateJobResult.taskConfig

  @mock.patch('apache.aurora.client.base.die')
  @mock.patch('apache.aurora.client.base.log')
  def test_synthesize_url(self, mock_log, mock_die):
    base_url = 'http://example.com'
    role = 'some-role'
    environment = 'some-environment'
    job = 'some-job'
    update_id = 'some-update-id'

    assert (('%s/scheduler/%s/%s/%s/update/%s' % (base_url, role, environment, job, update_id)) ==
        base.synthesize_url(base_url, role, environment, job, update_id=update_id))

    assert (('%s/scheduler/%s/%s/%s' % (base_url, role, environment, job)) ==
        base.synthesize_url(base_url, role, environment, job))

    assert (('%s/scheduler/%s/%s' % (base_url, role, environment)) ==
        base.synthesize_url(base_url, role, environment))

    assert (('%s/scheduler/%s' % (base_url, role)) ==
        base.synthesize_url(base_url, role))

    assert (('%s/scheduler/%s' % (base_url, role)) ==
        base.synthesize_url(base_url, role))

    mock_log.reset_mock()
    scheduler_url = ''
    out = base.synthesize_url(scheduler_url)
    self.assertIsNone(out)
    mock_log.warning.assert_called_once_with('Unable to find scheduler web UI!')

    mock_log.reset_mock()
    mock_die.reset_mock()
    scheduler_url = 'foo'
    env = 'foo'
    out = base.synthesize_url(scheduler_url, env=env)
    expected = 'scheduler'
    self.assertEqual(out, expected)
    mock_die.assert_called_once_with('If env specified, must specify role')

    mock_log.reset_mock()
    mock_die.reset_mock()
    scheduler_url = 'foo'
    job = 'bar'
    out = base.synthesize_url(scheduler_url, job=job)
    expected = 'scheduler'
    self.assertEqual(out, expected)
    mock_die.assert_called_once_with('If job specified, must specify role and env')

  @mock.patch('apache.aurora.client.base.pex_version')
  def test_user_agent(self, mock_pex_version):
    mock_pex_version.return_value = ('sha', '2015-11-3')
    out = base.user_agent()
    expected = 'Aurora;sha-2015-11-3'
    self.assertEqual(out, expected)

    mock_pex_version.reset_mock()
    mock_pex_version.side_effect = UnknownVersion
    out = base.user_agent()
    expected = 'Aurora;Unknown Version'
    self.assertEqual(out, expected)

  @mock.patch('apache.aurora.client.base.log.info')
  @mock.patch('apache.aurora.client.base.sys.exit')
  def test_check_and_log_response(self, mock_sys_exit, mock_log):
    resp = Response(responseCode=ResponseCode.LOCK_ERROR)
    out = base.check_and_log_response(resp)
    self.assertIsNone(out)
    mock_sys_exit.assert_called_once_with(1)
    mock_log.assert_any_call(base.LOCKED_WARNING)
    mock_log.assert_any_call('Response from scheduler: LOCK_ERROR (message: )')

  @mock.patch('apache.aurora.client.base.log.info')
  def test_check_and_log_locked_response(self, mock_log):
    resp = Response(responseCode=ResponseCode.LOCK_ERROR)
    out = base.check_and_log_locked_response(resp)
    self.assertIsNone(out)
    mock_log.assert_called_once_with(base.LOCKED_WARNING)

  @mock.patch('apache.aurora.client.base.sys.exit')
  @mock.patch('apache.aurora.client.base.log.fatal')
  def test_die(self, mock_log, mock_sys_exit):
    msg = 'fatal message'
    out = base.die(msg)
    self.assertIsNone(out)
    mock_sys_exit.assert_called_once_with(1)
    mock_log.assert_called_once_with(msg)
