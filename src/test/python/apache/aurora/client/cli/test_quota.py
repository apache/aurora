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

from gen.apache.aurora.api.ttypes import GetQuotaResult, Resource, ResourceAggregate, Result


class TestGetQuotaCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_quota_call_no_consumption(cls, mock_context):
    api = mock_context.get_api('west')
    response = cls.create_simple_success_response()
    response.result = Result(getQuotaResult=GetQuotaResult(
        quota=ResourceAggregate(resources=frozenset([
            Resource(numCpus=5),
            Resource(ramMb=20480),
            Resource(diskMb=40960)])),
        prodSharedConsumption=None,
        prodDedicatedConsumption=None,
        nonProdSharedConsumption=None,
        nonProdDedicatedConsumption=None
    ))
    api.get_quota.return_value = response

  @classmethod
  def setup_mock_quota_call_with_consumption(cls, mock_context):
    api = mock_context.get_api('west')
    response = cls.create_simple_success_response()
    response.result = Result(getQuotaResult=GetQuotaResult(
      quota=ResourceAggregate(resources=frozenset([
          Resource(numCpus=5),
          Resource(ramMb=20480),
          Resource(diskMb=40960)])),
      prodSharedConsumption=ResourceAggregate(resources=frozenset([
          Resource(numCpus=1),
          Resource(ramMb=512),
          Resource(diskMb=1024)])),
      prodDedicatedConsumption=ResourceAggregate(resources=frozenset([
          Resource(numCpus=2),
          Resource(ramMb=1024),
          Resource(diskMb=2048)])),
      nonProdSharedConsumption=ResourceAggregate(resources=frozenset([
          Resource(numCpus=3),
          Resource(ramMb=2048),
          Resource(diskMb=4096)])),
      nonProdDedicatedConsumption=ResourceAggregate(resources=frozenset([
          Resource(numCpus=4),
          Resource(ramMb=4096),
          Resource(diskMb=8192)])),
    ))
    api.get_quota.return_value = response

  def test_get_quota_no_consumption(self):
    assert ('Allocated:\n  CPU: 5 core(s)\n  RAM: 20480 MB\n  Disk: 40960 MB' ==
            self._get_quota(False, ['quota', 'get', 'west/bozo']))

  def test_get_quota_with_consumption(self):
    expected_output = ('Allocated:\n  CPU: 5 core(s)\n  RAM: 20480 MB\n  Disk: 40960 MB\n'
                       'Production shared pool resources consumed:\n'
                       '  CPU: 1 core(s)\n  RAM: 512 MB\n  Disk: 1024 MB\n'
                       'Production dedicated pool resources consumed:\n'
                       '  CPU: 2 core(s)\n  RAM: 1024 MB\n  Disk: 2048 MB\n'
                       'Non-production shared pool resources consumed:\n'
                       '  CPU: 3 core(s)\n  RAM: 2048 MB\n  Disk: 4096 MB\n'
                       'Non-production dedicated pool resources consumed:\n'
                       '  CPU: 4 core(s)\n  RAM: 4096 MB\n  Disk: 8192 MB')
    assert expected_output == self._get_quota(True, ['quota', 'get', 'west/bozo'])

  def test_get_quota_with_no_consumption_json(self):
    expected_response = self._response_converter(
        (json.loads('{"quota":{"resources":[{"diskMb":40960},{"numCpus":5},{"ramMb":20480}]}}')))
    actual_response = self._response_converter(
        json.loads(self._get_quota(False, ['quota', 'get', '--write-json', 'west/bozo'])))
    assert (expected_response == actual_response)

  def test_get_quota_with_consumption_json(self):
    expected_response = self._response_converter(json.loads(
        '{"quota":{"resources":[{"numCpus":5},{"ramMb":20480},{"diskMb":40960}]},'
        '"prodSharedConsumption":{"resources":[{"numCpus":1},{"ramMb":512},{"diskMb":1024}]},'
        '"prodDedicatedConsumption":{"resources":[{"numCpus":2},{"ramMb":1024},{"diskMb":2048}]},'
        '"nonProdSharedConsumption":{"resources":[{"numCpus":3},{"ramMb":2048},{"diskMb":4096}]},'
        '"nonProdDedicatedConsumption":{"resources":'
        '[{"numCpus":4},{"ramMb":4096},{"diskMb":8192}]}}'))
    assert (expected_response == self._response_converter(
      json.loads(self._get_quota(True, ['quota', 'get', '--write-json', 'west/bozo']))))

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

  def _response_converter(self, response):
    resources_list = []
    for key in response:
      if 'resources' in response[key] and response[key]['resources'] is not None:
        for resource in response[key]['resources']:
          resources_list += resource.items()
      response[key]['resources'] = sorted(resources_list, key=lambda x: (x[0], x[1]))
      resources_list = []
    return response
