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

from mock import patch

from apache.aurora.client.cli import EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext


class TestClientOpenCommand(AuroraClientCommandTest):

  def test_open_cluster(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'open', 'west'])
      assert mock_context.showed_urls == ['http://something_or_other/scheduler']
      assert result == EXIT_OK

  def test_open_role(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'open', 'west/bozo'])
      assert mock_context.showed_urls == ['http://something_or_other/scheduler/bozo']
      assert result == EXIT_OK

  def test_open_env(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'open', 'west/bozo/devel'])
      assert mock_context.showed_urls == ['http://something_or_other/scheduler/bozo/devel']
      assert result == EXIT_OK

  def test_open_job(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'open', 'west/bozo/devel/foo'])
      assert mock_context.showed_urls == ['http://something_or_other/scheduler/bozo/devel/foo']
      assert result == EXIT_OK

  def test_open_noparam(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context):
      cmd = AuroraCommandLine()
      self.assertRaises(SystemExit, cmd.execute, (['job', 'open']))
      assert mock_context.showed_urls == []
