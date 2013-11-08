import contextlib
import unittest

from twitter.aurora.common.cluster import Cluster
from twitter.aurora.common.clusters import Clusters
from twitter.aurora.client.commands.core import kill
from twitter.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from twitter.aurora.common.aurora_job_key import AuroraJobKey
from twitter.common.contextutil import temporary_file

from gen.twitter.aurora.ttypes import (
    Identity,
    Response,
    ResponseCode,
    Result,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery,
)

from mock import Mock, patch


class TestClientKllCommand(unittest.TestCase):
  # Configuration to use
  CONFIG_BASE = """
HELLO_WORLD = Job(
  name = '%s',
  role = '%s',
  cluster = '%s',
  environment = '%s',
  instances = 2,
  %s
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
  TEST_CLUSTER = 'smfd'

  TEST_CLUSTERS = Clusters([Cluster(
    name='smfd',
    packer_copy_command='copying {{package}}',
    zk='zookeeper.example.com',
    scheduler_zk_path='/foo/bar',
    auth_mechanism='UNAUTHENTICATED')])

  @classmethod
  def get_valid_config(cls):
    return cls.CONFIG_BASE % (cls.TEST_JOB, cls.TEST_ROLE, cls.TEST_CLUSTER, cls.TEST_ENV, '')

  @classmethod
  def get_invalid_config(cls, bad_clause):
    return cls.CONFIG_BASE % (cls.TEST_JOB, cls.TEST_ROLE, cls.TEST_CLUSTER, cls.TEST_ENV,
        bad_clause)

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.open_browser = False
    mock_options.shards = None
    mock_options.cluster = None
    mock_options.json = False
    return mock_options

  @classmethod
  def setup_mock_api_factory(cls):
    mock_api = Mock(spec=HookedAuroraClientAPI)
    mock_scheduler = Mock()
    mock_scheduler.url = "http://something_or_other"
    mock_api.scheduler = mock_scheduler
    mock_api_factory = Mock()
    mock_api_factory.return_value = mock_api
    mock_api.kill_job.return_value = cls.get_kill_job_response()
    mock_api.scheduler.scheduler.return_value = mock_scheduler
    return mock_api_factory

  @classmethod
  def create_mock_status_query_result(cls, scheduleStatus):
    mock_query_result = Mock(spec=Response)
    mock_query_result.result = Mock(spec=Result)
    mock_query_result.result.scheduleStatusResult = Mock(spec=ScheduleStatusResult)
    if scheduleStatus == ScheduleStatus.INIT:
      # status query result for before job is launched.
      mock_query_result.result.scheduleStatusResult.tasks = []
    else:
      mock_task_one = cls.create_mock_task('hello', 0, 1000, scheduleStatus)
      mock_task_two = cls.create_mock_task('hello', 1, 1004, scheduleStatus)
      mock_query_result.result.scheduleStatusResult.tasks = [mock_task_one, mock_task_two]
    return mock_query_result

  @classmethod
  def create_mock_query(cls):
    return TaskQuery(owner=Identity(role=cls.TEST_ROLE), environment=cls.TEST_ENV,
        jobName=cls.TEST_JOB)

  @classmethod
  def get_kill_job_response(cls):
    mock_resp = Mock(spec=Response)
    mock_resp.responseCode = ResponseCode.OK
    mock_resp.message = "OK"
    return mock_resp

  @classmethod
  def assert_kill_job_called(cls, mock_api):
    assert mock_api.kill_job.call_count == 1

  @classmethod
  def assert_scheduler_called(cls, mock_api):
    # scheduler.scheduler() is called once, as a part of the handle_open call.
    assert mock_api.scheduler.scheduler.call_count == 1

  def test_simple_successful_kill_job(self):
    """Run a test of the "kill" command against a mocked-out API:
    Verifies that the kill command sends the right API RPCs, and performs the correct
    tests on the result."""
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_api_factory = self.setup_mock_api_factory()
    with contextlib.nested(
        patch('twitter.aurora.client.commands.core.make_client_factory',
            return_value=mock_api_factory),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('twitter.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_make_client_factory,
            options, mock_get_job_config):
      mock_api = mock_api_factory.return_value

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['smfd/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      self.assert_kill_job_called(mock_api)
      mock_api.kill_job.assert_called_with(
        AuroraJobKey(cluster=self.TEST_CLUSTER, role=self.TEST_ROLE, env=self.TEST_ENV,
            name=self.TEST_JOB), None, config=mock_config)
      self.assert_scheduler_called(mock_api)
      assert mock_make_client_factory.call_count == 1

  @classmethod
  def setup_mock_api(cls):
    """Builds up a mock API object, with a mock SchedulerProxy.
    Returns the API and the proxy"""

    mock_scheduler = Mock()
    mock_scheduler.url = "http://something_or_other"
    mock_scheduler_client = Mock()
    mock_scheduler_client.scheduler.return_value = mock_scheduler
    mock_scheduler_client.url = "http://something_or_other"
    mock_api = Mock(spec=HookedAuroraClientAPI)
    mock_api.scheduler = mock_scheduler_client
    return (mock_api, mock_scheduler_client)

  @classmethod
  def get_expected_task_query(cls, shards=None):
    """Helper to create the query that will be a parameter to job kill."""
    instance_ids = frozenset(shards) if shards is not None else None
    return TaskQuery(taskIds=None, jobName=cls.TEST_JOB, environment=cls.TEST_ENV,
        instanceIds=instance_ids, owner=Identity(role=cls.TEST_ROLE, user=None))

  def test_kill_job_api_level(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler) = self.setup_mock_api()
    mock_api_factory = Mock(return_value=mock_api)
    mock_scheduler.killTasks.return_value = self.get_kill_job_response()
    with contextlib.nested(
        patch('twitter.aurora.client.factory.make_client_factory', return_value=mock_api_factory),
        patch('twitter.aurora.client.api.SchedulerProxy', return_value=mock_scheduler),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('twitter.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_api_factory_patch,
            mock_scheduler_proxy_class,
            mock_clusters,
            options, mock_get_job_config):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['smfd/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      self.assert_scheduler_called(mock_api)
      assert mock_scheduler.killTasks.call_count == 1
      mock_scheduler.killTasks.assert_called_with(self.get_expected_task_query(), None)

  def test_kill_job_api_level_with_shards(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_options.shards = [0, 1, 2, 3]
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler) = self.setup_mock_api()
    mock_api_factory = Mock(return_value=mock_api)
    mock_scheduler.killTasks.return_value = self.get_kill_job_response()
    with contextlib.nested(
        patch('twitter.aurora.client.factory.make_client_factory', return_value=mock_api_factory),
        patch('twitter.aurora.client.api.SchedulerProxy', return_value=mock_scheduler),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('twitter.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_api_factory_patch,
            mock_scheduler_proxy_class,
            mock_clusters,
            options, mock_get_job_config):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['smfd/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      self.assert_scheduler_called(mock_api)
      assert mock_scheduler.killTasks.call_count == 1
      query = self.get_expected_task_query([0, 1, 2, 3])
      mock_scheduler.killTasks.assert_called_with(query, None)
