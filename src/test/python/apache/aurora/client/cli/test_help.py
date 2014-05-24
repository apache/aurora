#
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
import unittest

from mock import patch

from apache.aurora.client.cli import EXIT_INVALID_PARAMETER, EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine


class TestHelp(unittest.TestCase):
  """Tests of the help command for the Aurora v2 client framework"""

  def setUp(self):
    self.cmd = AuroraCommandLine()
    self.transcript = []
    self.err_transcript = []

  def mock_print(self, str):
    for str in str.split('\n'):
      self.transcript.append(str)

  def mock_print_err(self, str):
    for str in str.split('\n'):
      self.err_transcript.append(str)

  def test_help(self):
    with patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
        side_effect=self.mock_print):
      self.cmd.execute(['help'])
      assert len(self.transcript) > 10
      assert self.transcript[1] == 'Usage:'
      assert '==Commands for jobs' in self.transcript
      assert '==Commands for quotas' in self.transcript

  def test_help_noun(self):
    with patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
        side_effect=self.mock_print):
      self.cmd.execute(['help', 'job'])
      assert len(self.transcript) > 10
      assert self.transcript[0] == 'Usage for noun "job":' in self.transcript
      assert not any('quota' in t for t in self.transcript)
      assert any('job status' in t for t in self.transcript)
      assert any('job list' in t for t in self.transcript)

  def test_help_verb(self):
    with patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
        side_effect=self.mock_print):
      assert self.cmd.execute(['help', 'job', 'status']) == EXIT_OK
      assert len(self.transcript) > 5
      assert self.transcript[0] == 'Usage for verb "job status":' in self.transcript
      assert not any('quota' in t for t in self.transcript)
      assert not any('list' in t for t in self.transcript)
      assert "Options:" in self.transcript
      assert any('status' for t in self.transcript)

  def test_help_unknown_noun(self):
    with contextlib.nested(
        patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
            side_effect=self.mock_print),
        patch('apache.aurora.client.cli.client.AuroraCommandLine.print_err',
            side_effect=self.mock_print_err)):
      assert self.cmd.execute(['help', 'nothing']) == EXIT_INVALID_PARAMETER
      assert len(self.transcript) == 0
      assert len(self.err_transcript) == 2
      assert 'Unknown noun "nothing"' == self.err_transcript[0]
      assert "Valid nouns" in self.err_transcript[1]

  def test_help_unknown_verb(self):
    with contextlib.nested(
        patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
            side_effect=self.mock_print),
        patch('apache.aurora.client.cli.client.AuroraCommandLine.print_err',
            side_effect=self.mock_print_err)):
      assert self.cmd.execute(['help', 'job', 'nothing']) == EXIT_INVALID_PARAMETER
      assert len(self.transcript) == 0
      assert len(self.err_transcript) == 2
      assert 'Noun "job" does not support a verb "nothing"' == self.err_transcript[0]
      assert 'Valid verbs for "job" are' in self.err_transcript[1]
