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

from mock import create_autospec, Mock, patch

from apache.aurora.client.commands.run import run

from .util import AuroraClientCommandTest

from gen.apache.aurora.api.constants import LIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    JobKey,
    ResponseCode,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)


class TestRunCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.num_threads = 4
    mock_options.tunnels = []
    mock_options.executor_sandbox = False
    mock_options.ssh_user = None
    mock_options.disable_all_hooks = False
    return mock_options

  @classmethod
  def create_mock_scheduled_tasks(cls):
    tasks = []
    for name in ['foo', 'bar', 'baz']:
      task = ScheduledTask(
        failureCount=0,
        status=ScheduleStatus.RUNNING,
        taskEvents=[
            TaskEvent(timestamp=123, status=ScheduleStatus.RUNNING, message='Fake message')],
        assignedTask=AssignedTask(
            assignedPorts={},
            slaveHost='slavehost',
            instanceId=0,
            taskId='taskid',
            task=TaskConfig(
                maxTaskFailures=1,
                executorConfig='fake data',
                metadata=[],
                owner=Identity(role='fakerole'),
                environment='test',
                jobName=name,
                numCpus=2,
                ramMb=2,
                diskMb=2
            )
        )
      )
      tasks.append(task)
    return tasks

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result.scheduleStatusResult = create_autospec(
        spec=ScheduleStatusResult,
        spec_set=False,
        instance=True)
    resp.result.scheduleStatusResult.tasks = cls.create_mock_scheduled_tasks()
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  @classmethod
  def create_mock_process(cls):
    process = Mock()
    process.communicate.return_value = ["hello", "world"]
    return process

  def test_successful_run(self):
    """Test the run command."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    sandbox_args = {'slave_root': '/slaveroot', 'slave_run_directory': 'slaverun'}
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.commands.run.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.command_runner.DistributedCommandRunner.sandbox_args',
            return_value=sandbox_args),
        patch('subprocess.Popen', return_value=self.create_mock_process())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            mock_clusters_runpatch,
            options,
            mock_runner_args_patch,
            mock_subprocess):
      run(['west/mchucarroll/test/hello', 'ls'], mock_options)

      # The status command sends a getTasksStatus query to the scheduler,
      # and then prints the result.
      mock_scheduler_proxy.getTasksStatus.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='mchucarroll', environment='test', name='hello')],
                    statuses=LIVE_STATES))

      # The mock status call returns 3 three ScheduledTasks, so three commands should have been run
      assert mock_subprocess.call_count == 3
      mock_subprocess.assert_called_with(['ssh', '-n', '-q', 'mchucarroll@slavehost',
          'cd /slaveroot/slaves/*/frameworks/*/executors/thermos-taskid/runs/'
          'slaverun/sandbox;ls'],
          stderr=-2, stdout=-1)
