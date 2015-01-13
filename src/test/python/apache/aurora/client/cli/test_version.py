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

from StringIO import StringIO

import pytest
from mock import patch

from apache.aurora.client.cli import __version__ as cli_version
from apache.aurora.client.cli.client import AuroraCommandLine

from .util import AuroraClientCommandTest


class TestClientVersionFlag(AuroraClientCommandTest):
  def test_version_flag(self):
    with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
      with pytest.raises(SystemExit):
        AuroraCommandLine().execute(['--version'])

      assert mock_stderr.getvalue() == cli_version
