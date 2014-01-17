import contextlib

from apache.aurora.client.commands.core import diff
from apache.aurora.client.commands.util import AuroraClientCommandTest
from twitter.common.contextutil import temporary_file

from gen.apache.aurora.constants import ACTIVE_STATES
from gen.apache.aurora.ttypes import (
    AssignedTask,
    ExecutorConfig,
    Identity,
    JobConfiguration,
    JobKey,
    PopulateJobResult,
    ResponseCode,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskEvent,
    TaskQuery,
)

from mock import Mock, patch
from pystachio.config import Config


class TestDiffCommand(AuroraClientCommandTest):
  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options = Mock()
    mock_options.env = None
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.rename_from = None
    mock_options.cluster = None
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
      job.assignedTask.task.executorConfig = Mock(spec=ExecutorConfig)
      job.assignedTask.task.executorConfig.data = Mock()
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

  @classmethod
  def setup_populate_job_config(cls, api):
    populate = cls.create_simple_success_response()
    populate.result.populateJobResult = Mock(spec=PopulateJobResult)
    api.populateJobConfig.return_value = populate
    populate.result.populateJobResult.populated = cls.create_mock_scheduled_tasks()
    return populate

  def test_successful_diff(self):
    """Test the diff command."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options,
            subprocess_patch,
            json_patch):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        diff(['west/mchucarroll/test/hello', fp.name])

        # Diff should get the task status, populate a config, and run diff.
        mock_scheduler_proxy.getTasksStatus.assert_called_with(
            TaskQuery(jobName='hello', environment='test', owner=Identity(role='mchucarroll'),
                statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 1
        assert isinstance(mock_scheduler_proxy.populateJobConfig.call_args[0][0], JobConfiguration)
        assert (mock_scheduler_proxy.populateJobConfig.call_args[0][0].key ==
            JobKey(environment=u'test', role=u'mchucarroll', name=u'hello'))
        # Subprocess should have been used to invoke diff with two parameters.
        assert subprocess_patch.call_count == 1
        assert len(subprocess_patch.call_args[0][0]) == 3
        assert subprocess_patch.call_args[0][0][0] == 'diff'

  def test_diff_invalid_config(self):
    """Test the diff command if the user passes a config with an error in it."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options,
            subprocess_patch,
            json_patch):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('stupid="me"',))
        fp.flush()
        self.assertRaises(Config.InvalidConfigError, diff,
            ['west/mchucarroll/test/hello', fp.name])
        assert mock_scheduler_proxy.getTasksStatus.call_count == 0
        assert mock_scheduler_proxy.populateJobConfig.call_count == 0
        assert subprocess_patch.call_count == 0

  def test_diff_server_error(self):
    """Test the diff command if the user passes a config with an error in it."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getTasksStatus.return_value = self.create_failed_status_response()
    self.setup_populate_job_config(mock_scheduler_proxy)
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('subprocess.call', return_value=0),
        patch('json.loads', return_value=Mock())) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options,
            subprocess_patch,
            json_patch):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        self.assertRaises(SystemExit, diff,
            ['west/mchucarroll/test/hello', fp.name])
        # In this error case, we should have called the server getTasksStatus;
        # but since it fails, we shouldn't call populateJobConfig or subprocess.
        mock_scheduler_proxy.getTasksStatus.assert_called_with(
            TaskQuery(jobName='hello', environment='test', owner=Identity(role='mchucarroll'),
                statuses=ACTIVE_STATES))
        assert mock_scheduler_proxy.populateJobConfig.call_count == 0
        assert subprocess_patch.call_count == 0
