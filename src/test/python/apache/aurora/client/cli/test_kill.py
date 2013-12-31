import contextlib
import unittest

from apache.aurora.client.cli import AuroraCommandLine
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from twitter.common.contextutil import temporary_file
from apache.aurora.client.cli.jobs import parse_instances
from apache.aurora.client.cli.util import AuroraClientCommandTest, FakeAuroraCommandContext

from gen.apache.aurora.ttypes import (
    Identity,
    TaskQuery,
)

from mock import Mock, patch


class TestInstancesParser(unittest.TestCase):
  def test_parse_instances(self):
    instances = '0,1-3,5'
    x = parse_instances(instances)
    assert x == [0, 1, 2, 3, 5]

  def test_parse_none(self):
    assert parse_instances(None) is None
    assert parse_instances("") is None


class TestClientKillCommand(AuroraClientCommandTest):
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
  def get_kill_job_response(cls):
    return cls.create_simple_success_response()

  @classmethod
  def assert_kill_job_called(cls, mock_api):
    assert mock_api.kill_job.call_count == 1

  def test_kill_job(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    mock_scheduler = Mock()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):

      api = mock_context.get_api('west')
      api.kill_job.return_value = self.get_kill_job_response()
      mock_scheduler.scheduler.killTasks.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, 'west/mchucarroll/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/mchucarroll/test/hello'), None)

  def test_kill_job_with_instances(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    with contextlib.nested(
        patch('apache.aurora.client.cli.jobs.Job.create_context', return_value=mock_context),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      api = mock_context.get_api('west')
      api.kill_job.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--instances=0,2,4-6',
           'west/mchucarroll/test/hello'])

      # Now check that the right API calls got made.
      assert api.kill_job.call_count == 1
      api.kill_job.assert_called_with(AuroraJobKey.from_path('west/mchucarroll/test/hello'),
          [0, 2, 4, 5, 6])

  def test_kill_job_with_instances_deep_api(self):
    """Test kill client-side API logic."""
    mock_context = FakeAuroraCommandContext()
    (mock_api, mock_scheduler) = self.setup_mock_api()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS)):
      mock_scheduler.killTasks.return_value = self.get_kill_job_response()
      with temporary_file() as fp:
        fp.write(self.get_valid_config())
        fp.flush()
        cmd = AuroraCommandLine()
        cmd.execute(['job', 'kill', '--config=%s' % fp.name, '--instances=0,2,4-6',
           'west/mchucarroll/test/hello'])
      # Now check that the right API calls got made.
      assert mock_scheduler.killTasks.call_count == 1
      mock_scheduler.killTasks.assert_called_with(
        TaskQuery(jobName='hello', environment='test', instanceIds=frozenset([0, 2, 4, 5, 6]),
            owner=Identity(role='mchucarroll')), None)


