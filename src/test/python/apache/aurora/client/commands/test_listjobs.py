#
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
import unittest

from mock import Mock, patch

from apache.aurora.client.commands.core import list_jobs
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters

from gen.apache.aurora.api.ttypes import GetJobsResult, JobKey


class TestListJobs(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls):
    """set up to get a mock options object."""
    mock_options = Mock()
    mock_options.pretty = False
    mock_options.show_cron = False
    mock_options.disable_all_hooks = False
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
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getJobs.return_value = self.create_listjobs_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options):
      list_jobs(['west/mchucarroll'])

      mock_scheduler_proxy.getJobs.assert_called_with(self.TEST_ROLE)

  def test_listjobs_badcluster(self):
    """Test the list_jobs command when the user provides an invalid cluster."""
    mock_options = self.setup_mock_options()
    (mock_api, mock_scheduler_proxy) = self.create_mock_api()
    mock_scheduler_proxy.getJobs.return_value = self.create_listjobs_response()
    with contextlib.nested(
        patch('apache.aurora.client.api.SchedulerProxy', return_value=mock_scheduler_proxy),
        patch('apache.aurora.client.factory.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)) as (
            mock_scheduler_proxy_class,
            mock_clusters,
            options):
      self.assertRaises(SystemExit, list_jobs, ['smoof/mchucarroll'])
