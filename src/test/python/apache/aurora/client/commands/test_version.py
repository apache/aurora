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
from __future__ import print_function

from mock import patch, PropertyMock

from apache.aurora.client.commands.core import version

from .util import AuroraClientCommandTest


class TestVersionCommand(AuroraClientCommandTest):

  @patch('__builtin__.print')
  @patch('twitter.common.python.pex.PexInfo.build_properties', new_callable=PropertyMock)
  def test_version_with_old_pants(self, mock_buildinfo, mock_print):
    # Old versions of pants wrote out sha and date keys
    mock_buildinfo.return_value = {'sha': 'foo', 'date': 'somedate'}
    version([])
    assert mock_print.call_count == 4
    calls = mock_print.mock_calls
    assert "foo" in calls[1][1][0]
    assert "somedate" in calls[2][1][0]

  @patch('__builtin__.print')
  @patch('twitter.common.python.pex.PexInfo.build_properties', new_callable=PropertyMock)
  def test_version_with_new_pants(self, mock_buildinfo, mock_print):
    # New versions of pants write out revision and datetime
    mock_buildinfo.return_value = {'revision': 'bar', 'datetime': 'somedatetime'}
    version([])
    assert mock_print.call_count == 4
    calls = mock_print.mock_calls
    assert "bar" in calls[1][1][0]
    assert "somedatetime" in calls[2][1][0]
