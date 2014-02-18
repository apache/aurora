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
from mock import Mock, patch

from apache.aurora.client.cli import EXIT_INVALID_PARAMETER
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.ttypes import (
    AssignedTask,
    Identity,
    JobKey,
    ResponseCode,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery,
)


class TestRunCommand(AuroraClientCommandTest):

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
      job.assignedTask.task.packages = []
      job.assignedTask.task.owner = Identity(role='bozo')
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

  @classmethod
  def create_mock_process(cls):
    process = Mock()
    process.communicate.return_value = ["hello", "world"]
    return process

  def test_successful_run(self):
    """Test the run command."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    sandbox_args = {'slave_root': '/slaveroot', 'slave_run_directory': 'slaverun'}
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.cli.task.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.command_runner.DistributedCommandRunner.sandbox_args',
            return_value=sandbox_args),
        patch('subprocess.Popen', return_value=self.create_mock_process())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            mock_clusters_cli,
            mock_runner_args_patch,
            mock_subprocess):
      cmd = AuroraCommandLine()
      cmd.execute(['task', 'run', 'west/bozo/test/hello', 'ls'])
      # The status command sends a getTasksStatus query to the scheduler,
      # and then prints the result.
      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='bozo'),
          statuses=set([ScheduleStatus.RUNNING, ScheduleStatus.KILLING, ScheduleStatus.RESTARTING,
              ScheduleStatus.PREEMPTING, ScheduleStatus.DRAINING])))

      # The mock status call returns 3 three ScheduledTasks, so three commands should have been run
      assert mock_subprocess.call_count == 3
      mock_subprocess.assert_called_with(['ssh', '-n', '-q', 'bozo@slavehost',
          'cd /slaveroot/slaves/*/frameworks/*/executors/thermos-1287391823/runs/'
          'slaverun/sandbox;ls'],
          stderr=-2, stdout=-1)


class TestSshCommand(AuroraClientCommandTest):
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
      job.assignedTask.task.packages = []
      job.assignedTask.task.owner = Identity(role='bozo')
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
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    sandbox_args = {'slave_root': '/slaveroot', 'slave_run_directory': 'slaverun'}
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.command_runner.DistributedCommandRunner.sandbox_args',
            return_value=sandbox_args),
        patch('subprocess.call', return_value=0)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            mock_runner_args_patch,
            mock_subprocess):
      cmd = AuroraCommandLine()
      cmd.execute(['task', 'ssh', 'west/bozo/test/hello/1', '--command=ls'])

      # The status command sends a getTasksStatus query to the scheduler,
      # and then prints the result.
      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='bozo'), instanceIds=set([1]),
          statuses=set([ScheduleStatus.RUNNING, ScheduleStatus.KILLING, ScheduleStatus.RESTARTING,
              ScheduleStatus.PREEMPTING, ScheduleStatus.DRAINING
              ])))
      mock_subprocess.assert_called_with(['ssh', '-t', 'bozo@slavehost',
          'cd /slaveroot/slaves/*/frameworks/*/executors/thermos-1287391823/runs/'
          'slaverun/sandbox;ls'])

