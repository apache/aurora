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

from unittest import TestCase
from zipfile import BadZipfile

from mock import call, create_autospec
from twitter.common.python.pex import PexInfo

from apache.aurora.client.commands.core import (
    _API_VERSION_MESSAGE,
    _BUILD_INFO_HEADER,
    _NO_BUILD_INFO_MESSAGE,
    _version
)


class TestVersionCommand(TestCase):

  def setUp(self):
    self.mock_print = create_autospec(print, spec_set=True)
    self.mock_from_pex = create_autospec(PexInfo.from_pex, spec_set=True)
    self.mock_argv = ['test-aurora.pex']

    self.mock_pex_info = create_autospec(PexInfo, instance=True, spec_set=True)

  def _invoke_version(self):
    _version(_argv=self.mock_argv, _print=self.mock_print, _from_pex=self.mock_from_pex)

  def test_version_with_old_pants(self):
    # Old versions of pants wrote out sha and date keys
    self.mock_pex_info.build_properties = {'sha': 'foo', 'date': 'somedate'}
    self.mock_from_pex.return_value = self.mock_pex_info

    self._invoke_version()

    self.mock_from_pex.assert_called_once_with(self.mock_argv[0])
    assert self.mock_print.call_count == 4
    calls = self.mock_print.mock_calls
    assert calls[0] == call(_BUILD_INFO_HEADER)
    assert "foo" in calls[1][1][0]
    assert "somedate" in calls[2][1][0]
    assert calls[3] == call(_API_VERSION_MESSAGE)

  def test_version_with_new_pants(self):
    # New versions of pants write out revision and datetime
    self.mock_pex_info.build_properties = {'revision': 'bar', 'datetime': 'somedatetime'}
    self.mock_from_pex.return_value = self.mock_pex_info

    self._invoke_version()

    self.mock_from_pex.assert_called_once_with(self.mock_argv[0])
    assert self.mock_print.call_count == 4
    calls = self.mock_print.mock_calls
    assert calls[0] == call(_BUILD_INFO_HEADER)
    assert "bar" in calls[1][1][0]
    assert "somedatetime" in calls[2][1][0]
    assert calls[3] == call(_API_VERSION_MESSAGE)

  def test_version_with_no_pants(self):
    # If we aren't a PEX we'll be a bad zip file.
    self.mock_from_pex.side_effect = BadZipfile

    self._invoke_version()

    self.mock_from_pex.assert_called_once_with(self.mock_argv[0])
    self.mock_print.assert_has_calls([
      call(_NO_BUILD_INFO_MESSAGE),
      call(_API_VERSION_MESSAGE),
    ])
