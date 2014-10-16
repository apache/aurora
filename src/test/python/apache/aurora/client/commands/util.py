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

import unittest
from collections import defaultdict

from mock import Mock

from apache.aurora.client.api.sla import DomainUpTimeSlaVector, JobUpTimeDetails
from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters

from gen.apache.aurora.api.ttypes import Response, ResponseCode, Result


class AuroraClientCommandTest(unittest.TestCase):
  @classmethod
  def create_blank_response(cls, code, msg):
    response = Mock(spec=Response)
    response.responseCode = code
    response.messageDEPRECATED = msg
    response.result = Mock(spec=Result)
    return response

  @classmethod
  def create_simple_success_response(cls):
    return cls.create_blank_response(ResponseCode.OK, 'OK')

  @classmethod
  def create_error_response(cls):
    return cls.create_blank_response(ResponseCode.ERROR, 'Damn')

  @classmethod
  def create_mock_api(cls):
    """Builds up a mock API object, with a mock SchedulerProxy"""
    # This looks strange, but we set up the same object to use as both
    # the SchedulerProxy and the SchedulerClient. These tests want to observe
    # what API calls get made against the scheduler, and both of these objects
    # delegate calls to the scheduler. It doesn't matter which one is used:
    # what we care about is that the right API calls get made.
    mock_scheduler_proxy = Mock()
    mock_scheduler_proxy.url = "http://something_or_other"
    mock_scheduler_proxy.scheduler_client.return_value = mock_scheduler_proxy
    mock_api = Mock(spec=HookedAuroraClientAPI)
    mock_api.scheduler_proxy = mock_scheduler_proxy
    return (mock_api, mock_scheduler_proxy)

  @classmethod
  def create_mock_api_factory(cls):
    """Create a collection of mocks for a test that wants to mock out the client API
    by patching the api factory."""
    mock_api, mock_scheduler_proxy = cls.create_mock_api()
    mock_api_factory = Mock()
    mock_api_factory.return_value = mock_api
    return mock_api_factory, mock_scheduler_proxy

  FAKE_TIME = 42131

  @classmethod
  def fake_time(cls, ignored):
    """Utility function used for faking time to speed up tests."""
    cls.FAKE_TIME += 2
    return cls.FAKE_TIME

  CONFIG_BASE = """
HELLO_WORLD = Job(
  name = '%(job)s',
  role = '%(role)s',
  cluster = '%(cluster)s',
  environment = '%(env)s',
  instances = 20,
  %(inner)s
  update_config = UpdateConfig(
    batch_size = 1,
    restart_threshold = 60,
    watch_secs = 45,
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
      zk='zookeeper.example.com',
      scheduler_zk_path='/foo/bar',
      auth_mechanism='UNAUTHENTICATED')])

  @classmethod
  def get_test_config(cls, cluster, role, env, job, filler=''):
    """Create a config from the template"""
    return cls.CONFIG_BASE % {'job': job, 'role': role, 'env': env, 'cluster': cluster,
        'inner': filler}

  @classmethod
  def get_valid_config(cls):
    return cls.get_test_config(cls.TEST_CLUSTER, cls.TEST_ROLE, cls.TEST_ENV, cls.TEST_JOB)

  @classmethod
  def get_invalid_config(cls, bad_clause):
    return cls.get_test_config(cls.TEST_CLUSTER, cls.TEST_ROLE, cls.TEST_ENV, cls.TEST_JOB,
        bad_clause)

  @classmethod
  def create_mock_probe_hosts_vector(cls, side_effects):
    mock_vector = Mock(spec=DomainUpTimeSlaVector)
    mock_vector.probe_hosts.side_effect = side_effects
    return mock_vector

  @classmethod
  def create_probe_hosts(cls, hostname, predicted, safe, safe_in):
    hosts = defaultdict(list)
    job = AuroraJobKey.from_path('west/role/env/job-%s' % hostname)
    hosts[hostname].append(JobUpTimeDetails(job, predicted, safe, safe_in))
    return [hosts]

  @classmethod
  def setup_mock_options(cls):
    mock_options = Mock(spec=['verbosity'])
    mock_options.verbosity = 'verbose'
    return mock_options
