#
# Copyright 2014 Apache Software Foundation
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

from mock import Mock, patch


class TestGetTaskUpCountCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_sla_uptime_vector(cls, mock_context, upcount):
    api = mock_context.get_api('west')
    response = Mock()
    response.get_task_up_count.return_value = upcount
    api.sla_get_job_uptime_vector.return_value = response

  def test_get_task_up_count_no_duration(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_sla_uptime_vector(mock_context, 10.6533333333)
    with contextlib.nested(
        patch('apache.aurora.client.cli.sla.Sla.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['sla', 'get_task_up_count', 'west/role/env/test'])
      out = '\n'.join(mock_context.get_out())
      assert '1 mins\t- 10.65 %\n' in out
      assert '10 mins\t- 10.65 %\n' in out
      assert '1 hrs\t- 10.65 %\n' in out
      assert '12 hrs\t- 10.65 %\n' in out
      assert '7 days\t- 10.65 %' in out

  def test_get_task_up_count_with_durations(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_sla_uptime_vector(mock_context, 95.3577434734)
    with contextlib.nested(
        patch('apache.aurora.client.cli.sla.Sla.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['sla', 'get_task_up_count', 'west/role/env/test', '--durations=3m,2d6h,3h'])
      out = '\n'.join(mock_context.get_out())
      assert '3 mins\t- 95.36 %' in out
      assert '54 hrs\t- 95.36 %' in out
      assert '3 hrs\t- 95.36 %' in out


class TestGetJobUptimeCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_sla_uptime_vector(cls, mock_context, uptime):
    api = mock_context.get_api('west')
    response = Mock()
    response.get_job_uptime.return_value = uptime
    api.sla_get_job_uptime_vector.return_value = response

  def test_get_job_uptime_no_percentile(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_sla_uptime_vector(mock_context, 915)
    with contextlib.nested(
        patch('apache.aurora.client.cli.sla.Sla.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['sla', 'get_job_uptime', 'west/role/env/test'])
      out = '\n'.join(mock_context.get_out())
      assert '99.0 percentile\t- 915 seconds' in out
      assert '95.0 percentile\t- 915 seconds' in out
      assert '90.0 percentile\t- 915 seconds' in out
      assert '85.0 percentile\t- 915 seconds' in out
      assert '75.0 percentile\t- 915 seconds' in out
      assert '60.0 percentile\t- 915 seconds' in out
      assert '50.0 percentile\t- 915 seconds' in out
      assert '30.0 percentile\t- 915 seconds' in out
      assert '10.0 percentile\t- 915 seconds' in out

  def test_get_job_uptime_with_percentiles(self):
    mock_context = FakeAuroraCommandContext()
    self.setup_mock_sla_uptime_vector(mock_context, 915)
    with contextlib.nested(
        patch('apache.aurora.client.cli.sla.Sla.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['sla', 'get_job_uptime', 'west/role/env/test', '--percentiles=99.9,85.5'])
      out = '\n'.join(mock_context.get_out())
      assert '99.9 percentile\t- 915 seconds' in out
      assert '85.5 percentile\t- 915 seconds' in out

  def test_invalid_percentile(self):
    cmd = AuroraCommandLine()
    try:
      cmd.execute(['sla', 'get_job_uptime', 'west/role/env/test', '--percentiles=100'])
    except SystemExit:
      pass
    else:
      assert 'Expected error is not raised.'
