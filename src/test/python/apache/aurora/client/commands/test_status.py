import contextlib
import unittest

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters
from apache.aurora.client.commands.core import status
from apache.aurora.client.commands.util import AuroraClientCommandTest

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

from mock import Mock, patch


class TestListJobs(AuroraClientCommandTest):
  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.pretty = False
    mock_options.show_cron = False
    return mock_options

  @classmethod
  def create_mock_scheduled_tasks(cls):
    jobs = []
    for name in ['foo', 'bar', 'baz']:
      job = Mock()
      job.key = JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=name)
      job.failure_count = 0
      job.assignedTask = Mock(spec=AssignedTask)
      job.assignedTask.slaveHost = 'slavehost'
      job.assignedTask.task = Mock(spec=TaskConfig)
      job.assignedTask.task.maxTaskFailures = 1
      job.assignedTask.task.packages = []
      job.assignedTask.task.owner = Identity(role='mchucarroll')
      job.assignedTask.task.environment = 'test'
      job.assignedTask.task.jobName = 'woops'
      job.assignedTask.task.numCpus = 2
      job.assignedTask.task.ramMb = 2
      job.assignedTask.task.diskMb = 2
      job.assignedTask.instanceId = 4237894
      job.assignedTask.assignedPorts = None
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
    resp.result.scheduleStatusResult.tasks = set(cls.create_mock_scheduled_tasks())
    return resp

  @classmethod
  def create_failed_status_response(cls):
    return cls.create_blank_response(ResponseCode.INVALID_REQUEST, 'No tasks found for query')

  def test_successful_status(self):
    """Test the status command."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options):
      status(['west/mchucarroll/test/hello'], mock_options)

      # The status command sends a getTasksStatus query to the scheduler,
      # and then prints the result.
      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='mchucarroll')))

  def test_unsuccessful_status(self):
    """Test the status command when the user asks the status of a job that doesn't exist."""
    # Calls api.check_status, which calls scheduler_proxy.getJobs
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_failed_status_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options):
      self.assertRaises(SystemExit, status, ['west/mchucarroll/test/hello'], mock_options)

      mock_scheduler_proxy.getTasksStatus.assert_called_with(TaskQuery(jobName='hello',
          environment='test', owner=Identity(role='mchucarroll')))
