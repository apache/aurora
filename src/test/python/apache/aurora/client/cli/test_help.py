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
  """Tests of the help command for the Aurora client framework"""

  def setUp(self):
    self.cmd = AuroraCommandLine()
    self.transcript = []
    self.err_transcript = []

  def mock_print(self, output, indent=0):
    self.transcript.extend(output.splitlines())

  def mock_print_err(self, output, indent=0):
    self.err_transcript.extend(output.splitlines())

  @contextlib.contextmanager
  def standard_mocks(self):
    with contextlib.nested(
        patch('apache.aurora.client.cli.client.AuroraCommandLine.print_out',
            side_effect=self.mock_print),
        patch('apache.aurora.client.cli.client.AuroraCommandLine.print_err',
            side_effect=self.mock_print_err),
        patch('apache.aurora.client.cli.get_client_version', return_value='0.0.test')):

      yield

  def test_all_help(self):
    for noun in self.cmd.registered_nouns:
      with self.standard_mocks():
        self.cmd.execute(['help', noun])
        assert 'Usage for noun "%s":' % noun in self.transcript
        assert self.err_transcript == []
        self.transcript = []
        for verb in self.cmd.nouns.get(noun).verbs.keys():
          self.cmd.execute(['help', noun, verb])
          assert 'Usage for verb "%s %s":' % (noun, verb) in self.transcript
          assert self.err_transcript == []
          self.transcript = []

  def test_help(self):
    with self.standard_mocks():
      self.cmd.execute(['help'])
      assert len(self.transcript) > 10
      assert self.transcript[0] == 'Aurora Client version 0.0.test'
      assert self.transcript[1] == 'Usage:'
      assert '==Commands for jobs' in self.transcript
      assert '==Commands for quotas' in self.transcript

  def test_usage_string_includes_plugin_options(self):
    plugin_options = []
    for plugin in self.cmd.plugins:
      if plugin.get_options() is not None:
        plugin_options += [p for p in plugin.get_options()]
    with self.standard_mocks():
      for noun in self.cmd.registered_nouns:
        for verb in self.cmd.nouns.get(noun).verbs.keys():
          self.transcript = []
          self.cmd.execute(['help', noun, verb])
          for opt in plugin_options:
            assert any(opt.name in line for line in self.transcript)

  def test_command_help_does_not_have_unset_str_metavars(self):
    plugin_options = []
    for plugin in self.cmd.plugins:
      if plugin.get_options() is not None:
        plugin_options += [p for p in plugin.get_options()]
    with self.standard_mocks():
      for noun in self.cmd.registered_nouns:
        for verb in self.cmd.nouns.get(noun).verbs.keys():
          self.transcript = []
          self.cmd.execute(['help', noun, verb])
          print(self.transcript)
          for opt in plugin_options:
            assert not any(line.endswith('=str') for line in self.transcript)

  def test_help_noun(self):
    with self.standard_mocks():
      self.cmd.execute(['help', 'job'])
      assert len(self.transcript) > 10
      assert self.transcript[0] == 'Usage for noun "job":' in self.transcript
      assert not any('quota' in t for t in self.transcript)
      assert any('job status' in t for t in self.transcript)
      assert any('job list' in t for t in self.transcript)

  def test_help_verb(self):
    with self.standard_mocks():
      assert self.cmd.execute(['help', 'job', 'status']) == EXIT_OK
      assert len(self.transcript) > 5
      assert self.transcript[0] == 'Usage for verb "job status":' in self.transcript
      assert not any('quota' in t for t in self.transcript)
      assert not any('list' in t for t in self.transcript)
      assert "Options:" in self.transcript
      assert any('status' for t in self.transcript)

  def test_help_unknown_noun(self):
    with self.standard_mocks():
      assert self.cmd.execute(['help', 'nothing']) == EXIT_INVALID_PARAMETER
      assert len(self.transcript) == 0
      assert len(self.err_transcript) == 2
      assert 'Unknown noun "nothing"' == self.err_transcript[0]
      assert "Valid nouns" in self.err_transcript[1]

  def test_help_unknown_verb(self):
    with self.standard_mocks():
      assert self.cmd.execute(['help', 'job', 'nothing']) == EXIT_INVALID_PARAMETER
      assert len(self.transcript) == 0
      assert len(self.err_transcript) == 2
      assert 'Noun "job" does not support a verb "nothing"' == self.err_transcript[0]
      assert 'Valid verbs for "job" are' in self.err_transcript[1]
