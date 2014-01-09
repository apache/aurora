import contextlib
import functools
import unittest

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters
from apache.aurora.client.commands.core import update
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.client.api.updater import Updater
from apache.aurora.client.api.health_check import InstanceWatcherHealthCheck, Retriable
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.config import AuroraConfig
from twitter.common.contextutil import temporary_file

from gen.apache.aurora.ttypes import (
    AcquireLockResult,
    AddInstancesConfig,
    AssignedTask,
    Identity,
    JobConfiguration,
    JobKey,
    PopulateJobResult,
    ResponseCode,
    ScheduledTask,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskConfig,
    TaskQuery
)

from mock import Mock, patch


class TestUpdateCommand(AuroraClientCommandTest):

  QUERY_STATUSES = frozenset([ScheduleStatus.PENDING, ScheduleStatus.STARTING,
      ScheduleStatus.RUNNING, ScheduleStatus.KILLING, ScheduleStatus.ASSIGNED,
      ScheduleStatus.RESTARTING, ScheduleStatus.PREEMPTING])

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.json = False
    mock_options.bindings = {}
    mock_options.open_browser = False
    mock_options.cluster = None
    mock_options.force = True
    mock_options.env = None
    mock_options.shards = None
    mock_options.health_check_interval_seconds = 3
    return mock_options

  @classmethod
  def setup_mock_updater(cls):
    updater = Mock(spec=Updater)
    return updater

  # First, we pretend that the updater isn't really client-side, and test
  # that the client makes the right API calls.
  def test_update_command_line_succeeds(self):
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.create_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (make_client,
          options,
          test_clusters):
      mock_api.update_job.return_value = self.create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        update([self.TEST_JOBSPEC, fp.name])

      assert mock_api.update_job.call_count == 1
      args, kwargs = mock_api.update_job.call_args
      assert isinstance(args[0], AuroraConfig)
      assert args[1] == 3
      assert args[2] is None

  def test_update_invalid_config(self):
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.create_mock_api()
    # Set up the context to capture the make_client and get_options calls.
    with contextlib.nested(
        patch('apache.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (make_client,
          options,
          test_clusters):
      with temporary_file() as fp:
        fp.write(self.get_invalid_config('invalid_field=False,'))
        fp.flush()
        self.assertRaises(AttributeError, update, ([self.TEST_JOBSPEC, fp.name]))

      assert mock_api.update_job.call_count == 0

  @classmethod
  def setup_mock_scheduler_for_simple_update(cls, api):
    """Set up all of the API mocks for scheduler calls during a simple update"""
    sched_proxy = api.scheduler.scheduler
    # First, the updater acquires a lock
    sched_proxy.acquireLock.return_value = cls.create_acquire_lock_response(ResponseCode.OK,
         'OK', 'token', False)
    # Then in gets the status of the tasks for the updating job.
    cls.setup_get_tasks_status_calls(sched_proxy)
    # Next, it needs to populate the update config.
    cls.setup_populate_job_config(sched_proxy)
    # Then it does the update, which kills and restarts jobs, and monitors their
    # health with the status call.
    cls.setup_kill_tasks(sched_proxy)
    cls.setup_add_tasks(sched_proxy)
    # Finally, after successful health checks, it releases the lock.
    cls.setup_release_lock_response(sched_proxy)

  @classmethod
  def setup_add_tasks(cls, api):
    add_response = cls.create_simple_success_response()
    api.addInstances.return_value = add_response
    return add_response

  @classmethod
  def setup_kill_tasks(cls, api):
    kill_response = cls.create_simple_success_response()
    api.killTasks.return_value = kill_response
    return kill_response

  @classmethod
  def setup_populate_job_config(cls, api):
    populate = cls.create_simple_success_response()
    populate.result.populateJobResult = Mock(spec=PopulateJobResult)
    api.populateJobConfig.return_value = populate
    configs = []
    for i in range(20):
      task_config = TaskConfig(numCpus=1.0, ramMb=1, diskMb=1)
      configs.append(task_config)
    populate.result.populateJobResult.populated = set(configs)
    return populate

  @classmethod
  def create_acquire_lock_response(cls, code, msg, token, rolling):
    """Set up the response to a startUpdate API call."""
    start_update_response = cls.create_blank_response(code, msg)
    start_update_response.result.acquireLockResult = Mock(spec=AcquireLockResult)
    start_update_response.result.acquireLockResult.lock = "foo"
    start_update_response.result.acquireLockResult.updateToken = 'token'
    return start_update_response

  @classmethod
  def setup_release_lock_response(cls, api):
    """Set up the response to a startUpdate API call."""
    release_lock_response = cls.create_simple_success_response()
    api.releaseLock.return_value = release_lock_response
    return release_lock_response

  @classmethod
  def setup_get_tasks_status_calls(cls, scheduler):
    status_response = cls.create_simple_success_response()
    scheduler.getTasksStatus.return_value = status_response
    schedule_status = Mock(spec=ScheduleStatusResult)
    status_response.result.scheduleStatusResult = schedule_status
    task_config = TaskConfig(numCpus=1.0, ramMb=10, diskMb=1)
    # This should be a list of ScheduledTask's.
    schedule_status.tasks = []
    for i in range(20):
      task_status = Mock(spec=ScheduledTask)
      task_status.assignedTask = Mock(spec=AssignedTask)
      task_status.assignedTask.instanceId = i
      task_status.assignedTask.taskId = "Task%s" % i
      task_status.assignedTask.slaveId = "Slave%s" % i
      task_status.slaveHost = "Slave%s" % i
      task_status.assignedTask.task = task_config
      schedule_status.tasks.append(task_status)

  @classmethod
  def assert_start_update_called(cls, mock_scheduler):
    assert mock_scheduler.scheduler.startUpdate.call_count == 1
    assert isinstance(mock_scheduler.scheduler.startUpdate.call_args[0][0], JobConfiguration)

  @classmethod
  def setup_health_checks(cls, mock_api):
    mock_health_check = Mock(spec=InstanceWatcherHealthCheck)
    mock_health_check.health.return_value = Retriable.alive()
    return mock_health_check

  def test_updater_simple(self):
    # Test the client-side updater logic in its simplest case: everything succeeds, and no rolling
    # updates.
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler.scheduler),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('apache.aurora.client.api.instance_watcher.InstanceWatcherHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=functools.partial(self.fake_time, self)),
        patch('time.sleep', return_value=None)

    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
          time_patch, sleep_patch):
      self.setup_mock_scheduler_for_simple_update(mock_api)
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        update(['west/mchucarroll/test/hello', fp.name])

      # We don't check all calls. The updater should be able to change. What's important
      # is that we verify the key parts of the update process, to have some confidence
      # that update did the right things.
      # Every update should:
      # check its options, acquire an update lock, check status,
      # kill the old tasks, start new ones, wait for the new ones to healthcheck,
      # and finally release the lock.
      # The kill/start should happen in rolling batches.
      assert options.call_count == 2
      assert mock_scheduler.scheduler.acquireLock.call_count == 1
      self.assert_correct_killtask_calls(mock_scheduler.scheduler)
      self.assert_correct_addinstance_calls(mock_scheduler.scheduler)
      self.assert_correct_status_calls(mock_scheduler.scheduler)
      assert mock_scheduler.scheduler.releaseLock.call_count == 1

  @classmethod
  def assert_correct_addinstance_calls(cls, api):
    assert api.addInstances.call_count == 4
    last_addinst = api.addInstances.call_args
    assert isinstance(last_addinst[0][0], AddInstancesConfig)
    assert last_addinst[0][0].instanceIds == frozenset([15, 16, 17, 18, 19])
    assert last_addinst[0][0].key == JobKey(environment='test', role='mchucarroll', name='hello')

  @classmethod
  def assert_correct_killtask_calls(cls, api):
    assert api.killTasks.call_count == 4
    # Check the last call's parameters.
    api.killTasks.assert_called_with(
        TaskQuery(taskIds=None, jobName='hello', environment='test',
            instanceIds=frozenset([16, 17, 18, 19, 15]),
            owner=Identity(role=u'mchucarroll', user=None),
           statuses=cls.QUERY_STATUSES),
        'foo')

  @classmethod
  def assert_correct_status_calls(cls, api):
    # getTasksStatus gets called a lot of times. The exact number isn't fixed; it loops
    # over the health checks until all of them pass for a configured period of time.
    # The minumum number of calls is 5: once before the tasks are restarted, and then
    # once for each batch of restarts (Since the batch size is set to 5, and the
    # total number of jobs is 20, that's 4 batches.)
    assert api.getTasksStatus.call_count >= 5
    # In the first getStatus call, it uses an expansive query; in the rest, it only queries for
    # status RUNNING.
    status_calls = api.getTasksStatus.call_args_list
    assert status_calls[0][0][0] == TaskQuery(taskIds=None, jobName='hello', environment='test',
        owner=Identity(role=u'mchucarroll', user=None),
        statuses=cls.QUERY_STATUSES)
    for status_call in status_calls[1:]:
      status_call[0][0] == TaskQuery(taskIds=None, jobName='hello', environment='test',
          owner=Identity(role='mchucarroll', user=None), statuses=set([ScheduleStatus.RUNNING]))
