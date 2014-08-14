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

from mock import Mock, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.client.cli.context import AuroraCommandContext
from apache.aurora.client.cli.util import AuroraClientCommandTest
from apache.aurora.config import AuroraConfig

from gen.apache.aurora.api.ttypes import CronCollisionPolicy


class TestInspectCommand(AuroraClientCommandTest):
  def get_mock_config(self):
    config = Mock(spec=AuroraConfig)
    # TODO(mchucarroll): figure out how to spec this. The raw_config is a Pystachio spec,
    # and that seems to blow up mock.
    raw_config = Mock()
    config.raw.return_value = raw_config
    raw_config.contact.return_value = "bozo@the.clown"
    raw_config.name.return_value = "the_job"
    raw_config.role.return_value = "bozo"
    raw_config.cluster.return_value = "west"
    raw_config.instances.return_value = 3
    raw_config.has_cron_schedule.return_value = True
    raw_config.cron_schedule.return_value = "* * * * *"
    raw_config.cron_collision_policy.return_value = CronCollisionPolicy.KILL_EXISTING
    raw_config.has_constraints.return_value = False
    raw_config.production.return_value.get.return_value = False
    mock_task = Mock()
    raw_config.task.return_value = mock_task
    mock_task.name.return_value = "task"
    mock_task.constraints.return_value.get.return_value = {}
    mock_process = Mock()
    mock_processes = [mock_process]
    mock_task.processes.return_value = mock_processes
    mock_process.name.return_value = "process"
    mock_process.daemon.return_value.get.return_value = False
    mock_process.ephemeral.return_value.get.return_value = False
    mock_process.final.return_value.get.return_value = False
    mock_process.cmdline.return_value.get.return_value = "ls -la"
    config.job.return_value.taskConfig.isService = False
    return config

  def test_inspect_job(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    AuroraCommandContext.enable_reveal_errors()
    mock_transcript = []
    def mock_print_out(msg, indent=0):
      indent_str = " " * indent
      mock_transcript.append("%s%s" % (indent_str, msg))
    with contextlib.nested(
        patch('threading._Event.wait'),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.cli.context.AuroraCommandContext.print_out',
            side_effect=mock_print_out),
        patch('apache.aurora.client.cli.context.get_config', return_value=self.get_mock_config())):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        result = cmd.execute(['job', 'inspect', '--reveal-errors', 'west/bozo/test/hello', fp.name])
        # inspect command should run without errors, and return 0.
        assert result == 0
        # The command output for the mock should look right.
        print(mock_transcript)
        assert mock_transcript == [
            "Job level information",
            "  name:       'the_job'",
            "  role:       'bozo'",
            "  contact:    'bozo@the.clown'",
            "  cluster:    'west'",
            "  instances:  '3'",
            "  cron:",
            "    schedule: '* * * * *'",
            "    policy:   '0'",
            "  service:    False",
            "  production: False",
            "",
            "Task level information",
            "  name: 'task'",
            "",
            "Process 'process':",
            "  cmdline:",
            "    ls -la",
            ""]
