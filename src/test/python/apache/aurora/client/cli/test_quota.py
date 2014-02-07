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

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.ttypes import (
    GetQuotaResult,
    Quota,
)

from mock import patch


class TestGetQuotaCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_quota_call_no_consumed(cls, mock_context):
    api = mock_context.get_api('west')
    response = cls.create_simple_success_response()
    response.result.getQuotaResult = GetQuotaResult()
    response.result.getQuotaResult.quota = Quota()
    response.result.getQuotaResult.quota.numCpus = 5
    response.result.getQuotaResult.quota.ramMb = 20480
    response.result.getQuotaResult.quota.diskMb = 40960
    response.result.getQuotaResult.consumed = None
    api.get_quota.return_value = response

  @classmethod
  def setup_mock_quota_call_with_consumed(cls, mock_context):
    api = mock_context.get_api('west')
    response = cls.create_simple_success_response()
    response.result.getQuotaResult = GetQuotaResult()
    response.result.getQuotaResult.quota = Quota()
    response.result.getQuotaResult.quota.numCpus = 5
    response.result.getQuotaResult.quota.ramMb = 20480
    response.result.getQuotaResult.quota.diskMb = 40960
    response.result.getQuotaResult.consumed = Quota()
    response.result.getQuotaResult.consumed.numCpus = 1
    response.result.getQuotaResult.consumed.ramMb = 1024
    response.result.getQuotaResult.consumed.diskMb = 2048
    api.get_quota.return_value = response

  def test_get_quota_no_consumed(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_quota_call_no_consumed(mock_context)
    with contextlib.nested(
        patch('apache.aurora.client.cli.quota.Quota.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['quota', 'get', 'west/bozo'])
      out = '\n'.join(mock_context.get_out())
      assert out == "Allocated:\n  CPU: 5\n  RAM: 20.000000 GB\n  Disk: 40.000000 GB"

  def test_get_quota_with_consumed(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_quota_call_with_consumed(mock_context)
    with contextlib.nested(
        patch('apache.aurora.client.cli.quota.Quota.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['quota', 'get', 'west/bozo'])
      out = '\n'.join(mock_context.get_out())
      assert out == ("Allocated:\n  CPU: 5\n  RAM: 20.000000 GB\n  Disk: 40.000000 GB\n"
          "Consumed:\n  CPU: 1\n  RAM: 1.000000 GB\n  Disk: 2.000000 GB")

  def test_get_quota_with_consumed_json(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_quota_call_no_consumed(mock_context)
    with contextlib.nested(
        patch('apache.aurora.client.cli.quota.Quota.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['quota', 'get', '--write_json', 'west/bozo'])
      out = '\n'.join(mock_context.get_out())
      assert out == '{"quota":{"numCpus":5,"ramMb":20480,"diskMb":40960}}'
