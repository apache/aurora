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
import textwrap

from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext
from mock import patch


class TestClientCreateCommand(AuroraClientCommandTest):

  def test_list_configs(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.config.ConfigNoun.create_context',
        return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['config', 'list', fp.name])
        assert mock_context.out == ['jobs=[west/bozo/test/hello]']
        assert mock_context.err == []

  def test_list_configs_invalid(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.config.ConfigNoun.create_context',
        return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config("blather=..."))
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['config', 'list', fp.name])
        assert mock_context.out == []
        assert any(line.startswith("Error loading configuration file: invalid syntax") for line in
            mock_context.err)

  def get_config_with_no_jobs(self):
    return textwrap.dedent("""
      HELLO_WORLD = Job(
        name = '%(job)s',
        role = '%(role)s',
        cluster = '%(cluster)s',
        environment = '%(env)s',
        instances = 20,
        update_config = UpdateConfig(
          batch_size = 5,
          restart_threshold = 30,
          watch_secs = 10,
          max_per_shard_failures = 2,
        ),
        task = Task(
          name = 'test',
          processes = [Process(name = 'hello_world', cmdline = 'echo {{thermos.ports[http]}}')],
          resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
        )
      )
      """)

  def test_list_configs_nojobs(self):
    mock_context = FakeAuroraCommandContext()
    with patch('apache.aurora.client.cli.config.ConfigNoun.create_context',
        return_value=mock_context):
      with temporary_file() as fp:
        fp.write(self.get_config_with_no_jobs())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['config', 'list', fp.name])
        assert mock_context.out == ["jobs=[]"]
        assert mock_context.err == []
