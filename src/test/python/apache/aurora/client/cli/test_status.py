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
import re
import textwrap

from mock import patch

from apache.aurora.client.cli import EXIT_INVALID_PARAMETER, EXIT_OK
from apache.aurora.client.cli.client import AuroraCommandLine
from apache.aurora.common.aurora_job_key import AuroraJobKey

from .util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.api.ttypes import (
    AssignedTask,
    GetJobsResult,
    Identity,
    JobConfiguration,
    JobKey,
    Metadata,
    ResponseCode,
    Result,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery
)


class TestJobStatus(AuroraClientCommandTest):
  @classmethod
  def create_scheduled_tasks(cls):
    tasks = AuroraClientCommandTest.create_scheduled_tasks()
    instance = 0
    for task in tasks:
      instance += 1
      task.assignedTask.instanceId = instance
      task.assignedTask.task.job = JobKey(role='bozo', environment='test', name='woops')
      task.assignedTask.task.jobName = 'woops'
    return tasks

  @classmethod
  def create_inactive_tasks(cls):
    instance = 0
    INACTIVE_STATUSES = [ScheduleStatus.KILLED, ScheduleStatus.FINISHED, ScheduleStatus.FAILED]
    tasks = cls.create_scheduled_tasks()
    for task in tasks:
      events = []
      for i in range(3):
        event = TaskEvent(
          timestamp=28234726395 + (273 * i),
          status=INACTIVE_STATUSES[i],
          message="Hi there")
        events.append(event)
      task.taskEvents = events
      task.status = INACTIVE_STATUSES[instance]
      task.assignedTask.instanceId = instance
      instance += 1
    return set(tasks)

  @classmethod
  def create_mock_scheduled_task_no_metadata(cls):
    result = cls.create_scheduled_tasks()
    for job in result:
      job.assignedTask.task.metadata = None
    return result

  @classmethod
  def create_mock_scheduled_task_with_metadata(cls):
    result = cls.create_scheduled_tasks()
    for job in result:
      job.assignedTask.task.metadata = [Metadata("meta", "data"), Metadata("data", "meta")]
    return result

  @classmethod
  def create_getjobs_response(cls):
    mock_job_one = JobConfiguration(
      key=JobKey(
        role='RoleA',
        environment='test',
        name='hithere'))
    mock_job_two = JobConfiguration(
      key=JobKey(
        role='bozo',
        environment='test',
        name='hello'))
    result = cls.create_simple_success_response()
    result.result = Result(
        getJobsResult=GetJobsResult(configs=[mock_job_one, mock_job_two]))
    return result

  @classmethod
  def create_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(tasks=set(cls.create_scheduled_tasks())))
    return resp

  @classmethod
  def create_status_null_metadata(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(
        scheduleStatusResult=ScheduleStatusResult(
            tasks=set(cls.create_mock_scheduled_task_no_metadata())))
    return resp

  @classmethod
  def create_status_with_inactives(cls):
    resp = cls.create_status_null_metadata()
    resp.result.scheduleStatusResult.tasks |= cls.create_inactive_tasks()
    return resp

  @classmethod
  def create_empty_status(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=None))
    return resp

  def get_task_status_json(cls):
    def create_task_events(start_time):
      """Create a list of task events, tracing the task from pending to assigned to running"""
      return [
          TaskEvent(timestamp=start_time, status=0, message="looking for a host"),
          TaskEvent(timestamp=start_time + 10, status=9, message="found a host"),
          TaskEvent(timestamp=start_time + 20, status=2, message="running")
      ]

    def create_scheduled_task(instance, start_time):
      task = ScheduledTask(
        assignedTask=AssignedTask(
          taskId="task_%s" % instance,
          slaveId="random_machine_id",
          slaveHost="junk.nothing",
          task=TaskConfig(
            job=JobKey(role="nobody", environment="prod", name='flibber'),
            owner=Identity(role="nobody"),
            environment="prod",
            jobName="flibber",
            isService=False,
            numCpus=2,
            ramMb=2048,
            diskMb=4096,
            priority=7,
            maxTaskFailures=3,
            production=False,
            requestedPorts=["http"]),
          assignedPorts={"http": 1001},
          instanceId=instance),
        status=2,
        failureCount=instance + 4,
        taskEvents=create_task_events(start_time),
        ancestorId="random_task_ancestor%s" % instance)
      return task

    resp = cls.create_simple_success_response()
    scheduleStatus = ScheduleStatusResult()
    scheduleStatus.tasks = [
      create_scheduled_task(0, 123456),
      create_scheduled_task(1, 234567)
    ]
    resp.result = Result(scheduleStatusResult=scheduleStatus)
    return resp

  @classmethod
  def create_status_with_metadata(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(
        tasks=set(cls.create_mock_scheduled_task_with_metadata())))
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  @classmethod
  def create_nojobs_status_response(cls):
    resp = cls.create_simple_success_response()
    resp.result = Result(scheduleStatusResult=ScheduleStatusResult(tasks=set()))
    return resp

  def test_successful_status_shallow(self):
    """Test the status command at the shallowest level: calling status should end up invoking
    the local APIs get_status method."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_api.check_status.assert_called_with(AuroraJobKey('west', 'bozo', 'test', 'hello'))

  def test_successful_status_shallow_nometadata(self):
    """Regression test: there was a crasher bug when metadata was None."""

    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_null_metadata()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_api.check_status.assert_called_with(AuroraJobKey('west', 'bozo', 'test', 'hello'))

  def test_successful_status_deep(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    _, mock_scheduler_proxy = self.create_mock_api()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_status_null_metadata()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_scheduler_proxy.getTasksWithoutConfigs.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')]))

  def test_successful_status_output_no_metadata(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    mock_context = FakeAuroraCommandContext()
    mock_context.add_expected_status_query_result(self.create_status_null_metadata())
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      actual = re.sub("\\d\\d:\\d\\d:\\d\\d", "##:##:##", '\n'.join(mock_context.get_out()))
      expected = textwrap.dedent("""\
          Active tasks (3):
          \tTask role: bozo, env: test, name: woops, instance: 1, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \tTask role: bozo, env: test, name: woops, instance: 2, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \tTask role: bozo, env: test, name: woops, instance: 3, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          Inactive tasks (0):
          """)
      assert actual == expected

  def test_successful_status_output_with_inactives(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    mock_context = FakeAuroraCommandContext()
    mock_context.add_expected_status_query_result(self.create_status_with_inactives())
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      actual = re.sub("\\d\\d:\\d\\d:\\d\\d", "##:##:##", '\n'.join(mock_context.get_out()))
      print("==actual======================\n%s\n========================" % actual)
      expected = textwrap.dedent("""\
          Active tasks (3):
          \tTask role: bozo, env: test, name: woops, instance: 1, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \tTask role: bozo, env: test, name: woops, instance: 2, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \tTask role: bozo, env: test, name: woops, instance: 3, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          Inactive tasks (3):
          \tTask role: bozo, env: test, name: woops, instance: 0, status: KILLED on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## KILLED: Hi there
          \t   1970-11-23 ##:##:## FINISHED: Hi there
          \t   1970-11-23 ##:##:## FAILED: Hi there
          \tTask role: bozo, env: test, name: woops, instance: 1, status: FINISHED on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## KILLED: Hi there
          \t   1970-11-23 ##:##:## FINISHED: Hi there
          \t   1970-11-23 ##:##:## FAILED: Hi there
          \tTask role: bozo, env: test, name: woops, instance: 2, status: FAILED on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## KILLED: Hi there
          \t   1970-11-23 ##:##:## FINISHED: Hi there
          \t   1970-11-23 ##:##:## FAILED: Hi there
          """)
      print("==expected======================\n%s\n========================" % expected)
      assert actual == expected

  def test_successful_status_output_with_metadata(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    mock_context = FakeAuroraCommandContext()
    mock_context.add_expected_status_query_result(self.create_status_with_metadata())
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      actual = re.sub("\\d\\d:\\d\\d:\\d\\d", "##:##:##", '\n'.join(mock_context.get_out()))
      expected = textwrap.dedent("""\
          Active tasks (3):
          \tTask role: bozo, env: test, name: woops, instance: 1, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \t  metadata:
          \t\t  (key: 'meta', value: 'data')
          \t\t  (key: 'data', value: 'meta')
          \tTask role: bozo, env: test, name: woops, instance: 2, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \t  metadata:
          \t\t  (key: 'meta', value: 'data')
          \t\t  (key: 'data', value: 'meta')
          \tTask role: bozo, env: test, name: woops, instance: 3, status: RUNNING on slavehost
          \t  cpus: 2, ram: 2 MB, disk: 2 MB
          \t  events:
          \t   1970-11-23 ##:##:## RUNNING: Hi there
          \t  metadata:
          \t\t  (key: 'meta', value: 'data')
          \t\t  (key: 'data', value: 'meta')
          Inactive tasks (0):
          """)
      print("=======actual======\n%s\n==================" % actual)
      print("==expected======================\n%s\n========================" % expected)
      assert actual == expected

  def test_successful_status_deep_null_metadata(self):
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksWithoutConfigs.return_value = self.create_status_null_metadata()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      mock_scheduler_proxy.getTasksWithoutConfigs.assert_called_with(
          TaskQuery(jobKeys=[JobKey(role='bozo', environment='test', name='hello')]))

  def test_status_wildcard(self):
    """Test status using a wildcard. It should first call api.get_jobs, and then do a
    getTasksWithoutConfigs on each job."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response()
    mock_api.get_jobs.return_value = self.create_getjobs_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.cli.context.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', '*'])

    # Wildcard should have expanded to two jobs, so there should be two calls
    # to check_status.
    assert mock_api.check_status.call_count == 2

    assert mock_api.check_status.call_args_list[0][0][0].cluster == 'west'
    assert mock_api.check_status.call_args_list[0][0][0].role == 'RoleA'
    assert mock_api.check_status.call_args_list[0][0][0].env == 'test'
    assert mock_api.check_status.call_args_list[0][0][0].name == 'hithere'

    assert mock_api.check_status.call_args_list[1][0][0].cluster == 'west'
    assert mock_api.check_status.call_args_list[1][0][0].role == 'bozo'
    assert mock_api.check_status.call_args_list[1][0][0].env == 'test'
    assert mock_api.check_status.call_args_list[1][0][0].name == 'hello'

  def test_status_wildcard_two(self):
    """Test status using a wildcard. It should first call api.get_jobs, and then do a
    getTasksWithoutConfigs on each job. This time, use a pattern that doesn't match
    all of the jobs."""
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_status_response()
    mock_api.get_jobs.return_value = self.create_getjobs_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', 'example/*/*/hello'])

    # Wildcard should have expanded to two jobs, but only matched one,
    # so there should be one call to check_status.
    assert mock_api.check_status.call_count == 1
    mock_api.check_status.assert_called_with(
        AuroraJobKey('example', 'bozo', 'test', 'hello'))

  def test_unsuccessful_status_shallow(self):
    """Test the status command at the shallowest level: calling status should end up invoking
    the local APIs get_status method."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_failed_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert result == EXIT_INVALID_PARAMETER

  def test_no_jobs_found_status_shallow(self):
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_nojobs_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'status', '--write-json', 'west/bozo/test/hello'])
      assert mock_context.get_out() == [
        '{"jobspec":"west/bozo/test/hello","error":"No matching jobs found"}']
      assert result == EXIT_OK

  def test_successful_status_json_output_no_metadata(self):
    """Test the status command more deeply: in a request with a fully specified
    job, it should end up doing a query using getTasksWithoutConfigs."""
    mock_context = FakeAuroraCommandContext()
    mock_context.add_expected_status_query_result(self.get_task_status_json())
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      cmd = AuroraCommandLine()
      cmd.execute(['job', 'status', '--write-json', 'west/bozo/test/hello'])
      actual = re.sub("\\d\\d:\\d\\d:\\d\\d", "##:##:##", '\n'.join(mock_context.get_out()))
      expected = textwrap.dedent("""\
        [
          {
            "active": [
              {
                "status": "RUNNING",
                "assignedTask": {
                  "task": {
                    "isService": false,
                    "environment": "prod",
                    "requestedPorts": [
                      "http"
                    ],
                    "jobName": "flibber",
                    "priority": 7,
                    "owner": {
                      "role": "nobody"
                    },
                    "job": {
                      "environment": "prod",
                      "role": "nobody",
                      "name": "flibber"
                    },
                    "production": false,
                    "diskMb": 4096,
                    "ramMb": 2048,
                    "maxTaskFailures": 3,
                    "numCpus": 2
                  },
                  "taskId": "task_0",
                  "instanceId": 0,
                  "assignedPorts": {
                    "http": 1001
                  },
                  "slaveHost": "junk.nothing",
                  "slaveId": "random_machine_id"
                },
                "ancestorId": "random_task_ancestor0",
                "taskEvents": [
                  {
                    "status": "PENDING",
                    "timestamp": 123456,
                    "message": "looking for a host"
                  },
                  {
                    "status": "ASSIGNED",
                    "timestamp": 123466,
                    "message": "found a host"
                  },
                  {
                    "status": "RUNNING",
                    "timestamp": 123476,
                    "message": "running"
                  }
                ],
                "failureCount": 4
              },
              {
                "status": "RUNNING",
                "assignedTask": {
                  "task": {
                    "isService": false,
                    "environment": "prod",
                    "requestedPorts": [
                      "http"
                    ],
                    "jobName": "flibber",
                    "priority": 7,
                    "owner": {
                      "role": "nobody"
                    },
                    "job": {
                      "environment": "prod",
                      "role": "nobody",
                      "name": "flibber"
                    },
                    "production": false,
                    "diskMb": 4096,
                    "ramMb": 2048,
                    "maxTaskFailures": 3,
                    "numCpus": 2
                  },
                  "taskId": "task_1",
                  "instanceId": 1,
                  "assignedPorts": {
                    "http": 1001
                  },
                  "slaveHost": "junk.nothing",
                  "slaveId": "random_machine_id"
                },
                "ancestorId": "random_task_ancestor1",
                "taskEvents": [
                  {
                    "status": "PENDING",
                    "timestamp": 234567,
                    "message": "looking for a host"
                  },
                  {
                    "status": "ASSIGNED",
                    "timestamp": 234577,
                    "message": "found a host"
                  },
                  {
                    "status": "RUNNING",
                    "timestamp": 234587,
                    "message": "running"
                  }
                ],
                "failureCount": 5
              }
            ],
            "job": "west/bozo/test/hello",
            "inactive": []
          }
        ]""")
      assert actual == expected

  def test_status_job_not_found(self):
    """Regression test: there was a crasher bug when metadata was None."""

    mock_context = FakeAuroraCommandContext()
    mock_api = mock_context.get_api('west')
    mock_api.check_status.return_value = self.create_empty_status()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context)):
      cmd = AuroraCommandLine()
      result = cmd.execute(['job', 'status', 'west/bozo/test/hello'])
      assert result == EXIT_INVALID_PARAMETER
      assert mock_context.get_err() == ["Found no jobs matching west/bozo/test/hello"]
