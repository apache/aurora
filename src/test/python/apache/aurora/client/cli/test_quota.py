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

from mock import patch

from apache.aurora.client.cli.client import AuroraCommandLine

from .util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.api.ttypes import GetQuotaResult, ResourceAggregate, Result


class TestGetQuotaCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_quota_call_no_consumption(cls, mock_context):
    api = mock_context.get_api('west')
    response = cls.create_simple_success_response()
    response.result = Result(getQuotaResult=GetQuotaResult(
        quota=ResourceAggregate(numCpus=5, ramMb=20480, diskMb=40960),
        prodSharedConsumption=None,
        nonProdSharedConsumption=None
    ))
    api.get_quota.return_value = response

  @classmethod
  def setup_mock_quota_call_with_consumption(cls, mock_context):
    api = mock_context.get_api('west')
    response = cls.create_simple_success_response()
    response.result = Result(getQuotaResult=GetQuotaResult(
      quota=ResourceAggregate(numCpus=5, ramMb=20480, diskMb=40960),
      prodSharedConsumption=ResourceAggregate(numCpus=1, ramMb=1024, diskMb=2048),
      nonProdSharedConsumption=ResourceAggregate(numCpus=1, ramMb=1024, diskMb=2048),
    ))
    api.get_quota.return_value = response

  def test_get_quota_no_consumption(self):
    assert ('Allocated:\n  CPU: 5\n  RAM: 20.000000 GB\n  Disk: 40.000000 GB' ==
            self._get_quota(False, ['quota', 'get', 'west/bozo']))

  def test_get_quota_with_consumption(self):
    expected_output = ('Allocated:\n  CPU: 5\n  RAM: 20.000000 GB\n  Disk: 40.000000 GB\n'
                       'Production resources consumed:\n'
                       '  CPU: 1\n  RAM: 1.000000 GB\n  Disk: 2.000000 GB\n'
                       'Non-production resources consumed:\n'
                       '  CPU: 1\n  RAM: 1.000000 GB\n  Disk: 2.000000 GB')
    assert expected_output == self._get_quota(True, ['quota', 'get', 'west/bozo'])

  def test_get_quota_with_no_consumption_json(self):
    assert (json.loads('{"quota":{"numCpus":5,"ramMb":20480,"diskMb":40960}}') ==
            json.loads(self._get_quota(False, ['quota', 'get', '--write-json', 'west/bozo'])))

  def test_get_quota_with_consumption_json(self):
    expected_response = json.loads(
        '{"quota":{"numCpus":5,"ramMb":20480,"diskMb":40960},'
        '"prodSharedConsumption":{"numCpus":1,"ramMb":1024,"diskMb":2048},'
        '"nonProdSharedConsumption":{"numCpus":1,"ramMb":1024,"diskMb":2048}}')
    assert (expected_response ==
            json.loads(self._get_quota(True, ['quota', 'get', '--write-json', 'west/bozo'])))

  def test_get_quota_failed(self):
    fake_context = FakeAuroraCommandContext()
    api = fake_context.get_api('')
    api.get_quota.return_value = self.create_error_response()

    self._call_get_quota(fake_context, ['quota', 'get', 'west/bozo'])

    assert fake_context.get_err() == ['Error retrieving quota for role bozo', '\tWhoops']

  def _get_quota(self, include_consumption, command_args):
    mock_context = FakeAuroraCommandContext()
    if include_consumption:
      self.setup_mock_quota_call_with_consumption(mock_context)
    else:
      self.setup_mock_quota_call_no_consumption(mock_context)

    return self._call_get_quota(mock_context, command_args)

  def _call_get_quota(self, mock_context, command_args):
    with patch('apache.aurora.client.cli.quota.Quota.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      cmd.execute(command_args)
      out = '\n'.join(mock_context.get_out())
      return out
