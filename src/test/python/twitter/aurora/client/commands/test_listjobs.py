import contextlib
import unittest

from twitter.aurora.common.cluster import Cluster
from twitter.aurora.common.clusters import Clusters
from twitter.aurora.client.commands.core import list_jobs
from twitter.aurora.client.commands.util import AuroraClientCommandTest

from gen.twitter.aurora.ttypes import (
    GetJobsResult,
    JobKey,
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
  def create_mock_jobs(cls):
    jobs = []
    for name in ['foo', 'bar', 'baz']:
      job = Mock()
      job.key = JobKey(role=cls.TEST_ROLE, environment=cls.TEST_ENV, name=name)
      jobs.append(job)
    return jobs

  @classmethod
  def create_listjobs_response(cls):
    resp = cls.create_simple_success_response()
    resp.result.getJobsResult = Mock(spec=GetJobsResult)
    resp.result.getJobsResult.configs = set(cls.create_mock_jobs())
    return resp

  def test_successful_listjobs(self):
    """Test the list_jobs command."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.create_mock_api()
    mock_scheduler.getJobs.return_value = self.create_listjobs_response()
    with contextlib.nested(
        patch('twitter.aurora.client.api.SchedulerProxy', return_value=mock_scheduler),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options):
      list_jobs(['west/mchucarroll'])

      mock_scheduler.getJobs.assert_called_with(self.TEST_ROLE)

  def test_listjobs_badcluster(self):
    """Test the list_jobs command when the user provides an invalid cluster."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler) = self.create_mock_api()
    mock_scheduler.getJobs.return_value = self.create_listjobs_response()
    with contextlib.nested(
        patch('twitter.aurora.client.api.SchedulerProxy', return_value=mock_scheduler),
        patch('twitter.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options):
      self.assertRaises(SystemExit, list_jobs, ['smoof/mchucarroll'])
