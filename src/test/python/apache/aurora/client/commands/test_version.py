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

from mock import call, create_autospec

from apache.aurora.client.commands.core import (
    _API_VERSION_MESSAGE,
    _BUILD_INFO_HEADER,
    _NO_BUILD_INFO_MESSAGE,
    _version
)
from apache.aurora.common.pex_version import pex_version, UnknownVersion


class TestVersionCommand(TestCase):

  def setUp(self):
    self.mock_print = create_autospec(print, spec_set=True)
    self.mock_pex_version = create_autospec(pex_version, spec_set=True)
    self.mock_argv = ['test-aurora.pex']

  def _invoke_version(self):
    _version(_argv=self.mock_argv, _print=self.mock_print, _pex_version=self.mock_pex_version)

  def test_version(self):
    self.mock_pex_version.return_value = ("foo", "somedate")

    self._invoke_version()

    self.mock_pex_version.assert_called_once_with(self.mock_argv[0])
    assert self.mock_print.call_count == 4
    calls = self.mock_print.mock_calls
    assert calls[0] == call(_BUILD_INFO_HEADER)
    assert "foo" in calls[1][1][0]
    assert "somedate" in calls[2][1][0]
    assert calls[3] == call(_API_VERSION_MESSAGE)

  def test_unknown_version(self):
    # If we aren't a PEX we'll be a bad zip file.
    self.mock_pex_version.side_effect = UnknownVersion

    self._invoke_version()

    self.mock_pex_version.assert_called_once_with(self.mock_argv[0])
    self.mock_print.assert_has_calls([
      call(_NO_BUILD_INFO_MESSAGE),
      call(_API_VERSION_MESSAGE),
    ])
