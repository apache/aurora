import contextlib
import unittest

from twitter.aurora.common.cluster import Cluster
from twitter.aurora.common.clusters import Clusters
from twitter.aurora.client.commands.core import update
from twitter.aurora.client.commands.util import (
    create_blank_response,
    create_mock_api,
    create_simple_success_response
)
from twitter.aurora.client.api.updater import Updater
from twitter.aurora.client.api.health_check import InstanceWatcherHealthCheck, Retriable
from twitter.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from twitter.aurora.config import AuroraConfig
from twitter.common.contextutil import temporary_file

from gen.twitter.aurora.ttypes import (
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


FAKE_TIME = 42131


def fake_time():
  global FAKE_TIME
  FAKE_TIME += 2
  return FAKE_TIME


class TestUpdateCommand(unittest.TestCase):
  CONFIG_BASE = """
HELLO_WORLD = Job(
  name = '%(job)s',
  role = '%(role)s',
  cluster = '%(cluster)s',
  environment = '%(env)s',
  instances = 20,
  %(inner)s
  update_config = UpdateConfig(
    batch_size = 5,
    restart_threshold = 30,
    watch_secs = 10,
    max_per_shard_failures = 2,
  ),
  task = Task(
    name = 'test',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{thermos.ports[http]}}')],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  )
)
jobs = [HELLO_WORLD]
"""

  TEST_ROLE = 'mchucarroll'
  TEST_ENV = 'test'
  TEST_JOB = 'hello'
  TEST_CLUSTER = 'west'
  TEST_JOBSPEC = 'west/mchucarroll/test/hello'

  TEST_CLUSTERS = Clusters([Cluster(
    name='west',
    packer_copy_command='copying {{package}}',
    zk='zookeeper.example.com',
    scheduler_zk_path='/foo/bar',
    auth_mechanism='UNAUTHENTICATED')])

  QUERY_STATUSES = frozenset([ScheduleStatus.PENDING, ScheduleStatus.STARTING,
      ScheduleStatus.RUNNING, ScheduleStatus.KILLING, ScheduleStatus.ASSIGNED,
      ScheduleStatus.RESTARTING, ScheduleStatus.PREEMPTING, ScheduleStatus.UPDATING,
      ScheduleStatus.ROLLBACK])

  @classmethod
  def get_config(cls, cluster, role, env, job, filler=''):
    """Create a config from the template"""
    return cls.CONFIG_BASE % {'job': job, 'role': role, 'env': env, 'cluster': cluster,
        'inner': filler}

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
    (mock_api, mock_scheduler) = create_mock_api()
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (make_client,
          options,
          test_clusters):
      mock_api.update_job.return_value = create_simple_success_response()

      with temporary_file() as fp:
        fp.write(self.get_config(self.TEST_CLUSTER, self.TEST_ROLE, self.TEST_ENV, self.TEST_JOB))
        fp.flush()
        update([self.TEST_JOBSPEC, fp.name])

      assert mock_api.update_job.call_count == 1
      args, kwargs = mock_api.update_job.call_args
      assert isinstance(args[0], AuroraConfig)
      assert args[1] == 3
      assert args[2] is None

  def test_update_invalid_config(self):
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = create_mock_api()
    # Set up the context to capture the make_client and get_options calls.
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client', return_value=mock_api),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)
    ) as (make_client,
          options,
          test_clusters):
      with temporary_file() as fp:
        fp.write(self.get_config(self.TEST_CLUSTER, self.TEST_ROLE, self.TEST_ENV, self.TEST_JOB,
            'invalid_field=False,'))
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
    add_response = create_blank_response(ResponseCode.OK, 'OK')
    api.addInstances.return_value = add_response
    return add_response

  @classmethod
  def setup_kill_tasks(cls, api):
    kill_response = create_blank_response(ResponseCode.OK, 'OK')
    api.killTasks.return_value = kill_response
    return kill_response

  @classmethod
  def setup_populate_job_config(cls, api):
    populate = create_blank_response(ResponseCode.OK, 'OK')
    populate.result.populateJobResult = Mock(spec=PopulateJobResult)
    api.populateJobConfig.return_value = populate
    configs = []
    for i in range(20):
      task_config = Mock(spec=TaskConfig)
      configs.append(task_config)
    populate.result.populateJobResult.populated = set(configs)
    return populate

  @classmethod
  def create_acquire_lock_response(cls, code, msg, token, rolling):
    """Set up the response to a startUpdate API call."""
    start_update_response = create_blank_response(code, msg)
    start_update_response.result.acquireLockResult = Mock(spec=AcquireLockResult)
    start_update_response.result.acquireLockResult.lock = "foo"
    start_update_response.result.acquireLockResult.updateToken = 'token'
    return start_update_response

  @classmethod
  def setup_release_lock_response(cls, api):
    """Set up the response to a startUpdate API call."""
    release_lock_response = create_blank_response(ResponseCode.OK, 'OK')
    api.releaseLock.return_value = release_lock_response
    return release_lock_response

  @classmethod
  def setup_get_tasks_status_calls(cls, scheduler):
    status_response = create_blank_response(ResponseCode.OK, 'OK')
    scheduler.getTasksStatus.return_value = status_response
    schedule_status = Mock(spec=ScheduleStatusResult)
    status_response.result.scheduleStatusResult = schedule_status
    mock_task_config = Mock()
    # This should be a list of ScheduledTask's.
    schedule_status.tasks = []
    for i in range(20):
      task_status = Mock(spec=ScheduledTask)
      task_status.assignedTask = Mock(spec=AssignedTask)
      task_status.assignedTask.instanceId = i
      task_status.assignedTask.taskId = "Task%s" % i
      task_status.assignedTask.slaveId = "Slave%s" % i
      task_status.slaveHost = "Slave%s" % i
      task_status.assignedTask.task = mock_task_config
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
    (mock_api, mock_scheduler) = create_mock_api()
    mock_health_check = self.setup_health_checks(mock_api)

    with contextlib.nested(
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('twitter.aurora.client.api.SchedulerProxy', return_value=mock_scheduler.scheduler),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.aurora.client.api.instance_watcher.InstanceWatcherHealthCheck',
            return_value=mock_health_check),
        patch('time.time', side_effect=fake_time),
        patch('time.sleep', return_value=None)

    ) as (options, scheduler_proxy_class, test_clusters, mock_health_check_factory,
          time_patch, sleep_patch):
      self.setup_mock_scheduler_for_simple_update(mock_api)
      with temporary_file() as fp:
        fp.write(self.get_config(self.TEST_CLUSTER, self.TEST_ROLE, self.TEST_ENV, self.TEST_JOB))
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
