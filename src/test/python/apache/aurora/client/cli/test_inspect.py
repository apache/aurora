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
import json

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

  def test_inspect_job_in_json(self):
    mock_stdout = []
    def mock_print_out(msg):
      mock_stdout.append("%s" % msg)
    with contextlib.nested(
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=mock_print_out),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.get_job_config',
            return_value=self.get_job_config())):
      cmd = AuroraCommandLine()
      assert cmd.execute([
          'job', 'inspect', '--write-json', 'west/bozo/test/hello', 'config.aurora']) == 0
    output = {
        "environment": "test",
        "health_check_config": {
            "initial_interval_secs": 15.0,
            "health_checker": {
                "http": {
                    "expected_response_code": 0,
                    "endpoint": "/health",
                    "expected_response": "ok"}},
            "interval_secs": 10.0,
            "timeout_secs": 1.0,
            "max_consecutive_failures": 0,
            "min_consecutive_successes": 1},
        "cluster": "west",
        "cron_schedule": "* * * * *",
        "service": False,
        "update_config": {
            "wait_for_batch_completion": False,
            "batch_size": 1,
            "watch_secs": 45,
            "rollback_on_failure": True,
            "max_per_shard_failures": 0,
            "max_total_failures": 0},
        "name": "the_job",
        "max_task_failures": 1,
        "cron_collision_policy": "KILL_EXISTING",
        "enable_hooks": False,
        "instances": 3,
        "task": {
            "processes": [{
                "daemon": False,
                "name": "process",
                "ephemeral": False,
                "max_failures": 1,
                "min_duration": 5,
                "cmdline": "ls -la",
                "final": False}],
            "name": "task",
            "finalization_wait": 30,
            "max_failures": 1,
            "max_concurrency": 0,
            "resources": {
                 "gpu": 0,
                 "disk": 1073741824,
                 "ram": 1073741824,
                 "cpu": 1.0},
            "constraints": []},
        "production": False,
        "role": "bozo",
        "contact": "bozo@the.clown",
        "metadata": [],
        "lifecycle": {
            "http": {
                "graceful_shutdown_endpoint": "/quitquitquit",
                "port": "health",
                "shutdown_endpoint": "/abortabortabort",
                "graceful_shutdown_wait_secs": 5,
                "shutdown_wait_secs": 5}},
        "priority": 0}

    mock_output = "\n".join(mock_stdout)
    assert output == json.loads(mock_output)

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
      # It's impossible to assert string equivalence of two objects with nested un-hashable types.
      # Given that the only product of --raw flag is the thrift representation of AuroraConfig
      # it's enough to do a spot check here and let thrift.py tests validate the structure.
      assert 'TaskConfig' in output

  # AURORA-990: Prevent regression of client passing invalid arguments to print_out.
  # Since print_out is the final layer before print(), there's not much else we can do than
  # ensure the command exits normally.
  def test_inspect_job_raw_success(self):
    with patch('apache.aurora.client.cli.context.AuroraCommandContext.get_job_config',
            return_value=self.get_job_config()):
      cmd = AuroraCommandLine()
      assert cmd.execute(['job', 'inspect', '--raw', 'west/bozo/test/hello', 'config.aurora']) == 0
