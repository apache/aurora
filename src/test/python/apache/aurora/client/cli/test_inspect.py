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

from mock import patch

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.config import AuroraConfig
from apache.aurora.config.schema.base import Job
from apache.thermos.config.schema_base import MB, Process, Resources, Task

from .util import AuroraClientCommandTest


class TestInspectCommand(AuroraClientCommandTest):
  def get_job_config(self):
    return AuroraConfig(job=Job(
      cluster='west',
      role='bozo',
      environment='test',
      name='the_job',
      service=False,
      task=Task(
        name='task',
        processes=[Process(cmdline='ls -la', name='process')],
        resources=Resources(cpu=1.0, ram=1024 * MB, disk=1024 * MB)
      ),
      contact='bozo@the.clown',
      instances=3,
      cron_schedule='* * * * *'
    ))

  def test_inspect_job(self):
    mock_stdout = []
    def mock_print_out(msg, indent=0):
      indent_str = " " * indent
      mock_stdout.append("%s%s" % (indent_str, msg))
    with contextlib.nested(
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=mock_print_out),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.get_job_config',
            return_value=self.get_job_config())):
      cmd = AuroraCommandLine()
      assert cmd.execute(['job', 'inspect', 'west/bozo/test/hello', 'config.aurora']) == 0
      output = '\n'.join(mock_stdout)
      assert output == '''Job level information
  name:       'the_job'
  role:       'bozo'
  contact:    'bozo@the.clown'
  cluster:    'west'
  instances:  '3'
  cron:
    schedule: '* * * * *'
    policy:   'KILL_EXISTING'
  service:    False
  production: False

Task level information
  name: 'task'

Process 'process':
  cmdline:
    ls -la
'''

  def test_inspect_job_raw(self):
    mock_stdout = []
    def mock_print_out(msg, indent=0):
      indent_str = " " * indent
      mock_stdout.append("%s%s" % (indent_str, msg))
    job_config = self.get_job_config()
    with contextlib.nested(
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=mock_print_out),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.get_job_config',
            return_value=job_config)):
      cmd = AuroraCommandLine()
      assert cmd.execute(['job', 'inspect', '--raw', 'west/bozo/test/hello', 'config.aurora']) == 0
      output = '\n'.join(mock_stdout)
      assert output == str(job_config.job())

  # AURORA-990: Prevent regression of client passing invalid arguments to print_out.
  # Since print_out is the final layer before print(), there's not much else we can do than
  # ensure the command exits normally.
  def test_inspect_job_raw_success(self):
    with patch('apache.aurora.client.cli.context.AuroraCommandContext.get_job_config',
            return_value=self.get_job_config()):
      cmd = AuroraCommandLine()
      assert cmd.execute(['job', 'inspect', '--raw', 'west/bozo/test/hello', 'config.aurora']) == 0
