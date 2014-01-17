import contextlib
import unittest

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters
from apache.aurora.client.commands.core import kill
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from twitter.common.contextutil import temporary_file
from apache.aurora.client.commands.util import AuroraClientCommandTest

from gen.apache.aurora.ttypes import (
    Identity,
    ScheduleStatus,
    ScheduleStatusResult,
    TaskQuery,
)

from mock import Mock, patch


class TestClientKillCommand(AuroraClientCommandTest):

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
    mock_api_factory, mock_api = cls.create_mock_api_factory()
    mock_api_factory.return_value.kill_job.return_value = cls.get_kill_job_response()
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
    return cls.create_simple_success_response()

  @classmethod
  def assert_kill_job_called(cls, mock_api):
    assert mock_api.kill_job.call_count == 1


  def test_simple_successful_kill_job(self):
    """Run a test of the "kill" command against a mocked-out API:
    Verifies that the kill command sends the right API RPCs, and performs the correct
    tests on the result."""
    mock_options = self.setup_mock_options()
    mock_config = Mock()
    mock_api_factory = self.setup_mock_api_factory()
    with contextlib.nested(
        patch('apache.aurora.client.commands.core.make_client_factory',
            return_value=mock_api_factory),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_make_client_factory,
            options, mock_get_job_config):
      mock_api = mock_api_factory.return_value

      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      self.assert_kill_job_called(mock_api)
      mock_api.kill_job.assert_called_with(
        AuroraJobKey(cluster=self.TEST_CLUSTER, role=self.TEST_ROLE, env=self.TEST_ENV,
            name=self.TEST_JOB), None, config=mock_config)
      assert mock_make_client_factory.call_count == 1

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
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api_factory = Mock(return_value=mock_api)
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    with contextlib.nested(
        patch('apache.aurora.client.factory.make_client_factory', return_value=mock_api_factory),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_api_factory_patch,
            mock_scheduler_proxy_class,
            mock_clusters,
            options, mock_get_job_config):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      mock_scheduler_proxy.killTasks.assert_called_with(self.get_expected_task_query(), None)

  def test_kill_job_api_level_with_shards(self):
    """Test kill client-side API logic."""
    mock_options = self.setup_mock_options()
    mock_options.shards = [0, 1, 2, 3]
    mock_config = Mock()
    mock_config.hooks = []
    mock_config.raw.return_value.enable_hooks.return_value.get.return_value = False
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_api_factory = Mock(return_value=mock_api)
    mock_scheduler_proxy.killTasks.return_value = self.get_kill_job_response()
    with contextlib.nested(
        patch('apache.aurora.client.factory.make_client_factory', return_value=mock_api_factory),
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options),
        patch('apache.aurora.client.commands.core.get_job_config', return_value=mock_config)) as (
            mock_api_factory_patch,
            mock_scheduler_proxy_class,
            mock_clusters,
            options, mock_get_job_config):
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        kill(['west/mchucarroll/test/hello', fp.name], mock_options)

      # Now check that the right API calls got made.
      assert mock_scheduler_proxy.killTasks.call_count == 1
      query = self.get_expected_task_query([0, 1, 2, 3])
      mock_scheduler_proxy.killTasks.assert_called_with(query, None)
