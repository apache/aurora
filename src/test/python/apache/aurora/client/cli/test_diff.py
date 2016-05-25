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

from mock import Mock, call, patch
from pystachio import Empty

from apache.aurora.client.cli import EXIT_OK
from apache.aurora.client.cli.diff_formatter import DiffFormatter
from apache.aurora.client.cli.jobs import DiffCommand
from apache.aurora.client.cli.options import TaskInstanceKey
from apache.aurora.config import AuroraConfig
from apache.aurora.config.schema.base import Job
from apache.thermos.config.schema_base import MB, Process, Resources, Task

from .util import AuroraClientCommandTest, FakeAuroraCommandContext, mock_verb_options

from gen.apache.aurora.api.ttypes import PopulateJobResult, Result


class TestDiffCommand(AuroraClientCommandTest):
  def setUp(self):
    self._command = DiffCommand()
    self._mock_options = mock_verb_options(self._command)
    self._mock_options.instance_spec = TaskInstanceKey(self.TEST_JOBKEY, [0, 1])
    self._fake_context = FakeAuroraCommandContext()
    self._fake_context.set_options(self._mock_options)
    self._mock_api = self._fake_context.get_api("test")
    self._formatter = Mock(spec=DiffFormatter)

  @classmethod
  def get_job_config(self, is_cron=False):
    return AuroraConfig(job=Job(
      cluster='west',
      role='bozo',
      environment='test',
      name='the_job',
      service=True if not is_cron else False,
      cron_schedule='* * * * *' if is_cron else Empty,
      task=Task(
        name='task',
        processes=[Process(cmdline='ls -la', name='process')],
        resources=Resources(cpu=1.0, ram=1024 * MB, disk=1024 * MB)
      ),
      instances=3,
    ))

  @classmethod
  def populate_job_config_result(cls):
    populate = cls.create_simple_success_response()
    populate.result = Result(populateJobResult=PopulateJobResult(
        taskConfig=cls.create_scheduled_tasks()[0].assignedTask.task))
    return populate

  def test_service_diff(self):
    config = self.get_job_config()
    self._fake_context.get_job_config = Mock(return_value=config)
    resp = self.populate_job_config_result()
    self._mock_api.populate_job_config.return_value = resp

    with patch('apache.aurora.client.cli.jobs.DiffFormatter') as formatter:
      formatter.return_value = self._formatter
      assert self._command.execute(self._fake_context) == EXIT_OK

    assert self._mock_api.populate_job_config.mock_calls == [call(config)]
    assert self._formatter.show_job_update_diff.mock_calls == [
      call(self._mock_options.instance_spec.instance, resp.result.populateJobResult.taskConfig)
    ]
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == []

  def test_cron_diff(self):
    config = self.get_job_config(is_cron=True)
    self._fake_context.get_job_config = Mock(return_value=config)
    resp = self.populate_job_config_result()
    self._mock_api.populate_job_config.return_value = resp

    with patch('apache.aurora.client.cli.jobs.DiffFormatter') as formatter:
      formatter.return_value = self._formatter
      assert self._command.execute(self._fake_context) == EXIT_OK

    assert self._mock_api.populate_job_config.mock_calls == [call(config)]
    assert self._formatter.diff_no_update_details.mock_calls == [
      call([resp.result.populateJobResult.taskConfig] * 3)
    ]
    assert self._fake_context.get_out() == []
    assert self._fake_context.get_err() == []
