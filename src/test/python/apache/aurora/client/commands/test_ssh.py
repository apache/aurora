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

from apache.aurora.client.commands.ssh import ssh
from apache.aurora.client.commands.util import AuroraClientCommandTest

from gen.apache.aurora.api.constants import LIVE_STATES
from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    Identity,
    JobKey,
    ResponseCode,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)


class TestSshCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.tunnels = []
    mock_options.executor_sandbox = False
    mock_options.ssh_user = None
    mock_options.disable_all_hooks = False
    return mock_options

  @classmethod
  def create_mock_scheduled_tasks(cls):
    jobs = []
    for name in ['foo', 'bar', 'baz']:
      job = Mock()
      job.key = JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=name)
      job.failure_count = 0
      job.assignedTask = Mock(spec=AssignedTask)
      job.assignedTask.taskId = 1287391823
      job.assignedTask.slaveHost = 'slavehost'
      job.assignedTask.task = Mock(spec=TaskConfig)
      job.assignedTask.task.executorConfig = Mock()
      job.assignedTask.task.maxTaskFailures = 1
      job.assignedTask.task.metadata = []
      job.assignedTask.task.owner = Identity(role='mchucarroll')
      job.assignedTask.task.environment = 'test'
      job.assignedTask.task.jobName = 'woops'
      job.assignedTask.task.numCpus = 2
      job.assignedTask.task.ramMb = 2
      job.assignedTask.task.diskMb = 2
      job.assignedTask.instanceId = 4237894
      job.assignedTask.assignedPorts = {}
      job.status = ScheduleStatus.RUNNING
      mockEvent = Mock(spec=TaskEvent)
      mockEvent.timestamp = 28234726395
      mockEvent.status = ScheduleStatus.RUNNING
      mockEvent.message = "Hi there"
      job.taskEvents = [mockEvent]
      jobs.append(job)
    return jobs

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    resp.result.scheduleStatusResult.tasks = cls.create_mock_scheduled_tasks()
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  def test_successful_ssh(self):
    """Test the ssh command."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    sandbox_args = {'slave_root': '/slaveroot', 'slave_run_directory': 'slaverun'}
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.command_runner.DistributedCommandRunner.sandbox_args',
            return_value=sandbox_args),
        patch('subprocess.call', return_value=0)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options,
            mock_runner_args_patch,
            mock_subprocess):
      ssh(['west/mchucarroll/test/hello', '1', 'ls'], mock_options)

      # The status command sends a getTasksStatus query to the scheduler,
      # and then prints the result.
      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='mchucarroll'), instanceIds=set([1]),
          statuses=LIVE_STATES))
      mock_subprocess.assert_called_with(['ssh', '-t', 'mchucarroll@slavehost',
          'cd /slaveroot/slaves/*/frameworks/*/executors/thermos-1287391823/runs/'
          'slaverun/sandbox;ls'])
