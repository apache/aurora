import unittest

from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters

from gen.apache.aurora.ttypes import (
    Response,
    ResponseCode,
    Result,
)

from mock import Mock


class AuroraClientCommandTest(unittest.TestCase):
  @classmethod
  def create_blank_response(cls, code, msg):
    response = Mock(spec=Response)
    response.responseCode = code
    response.message = msg
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
    mock_api = Mock(spec=HookedAuroraClientAPI)
    mock_scheduler = Mock()
    mock_scheduler.url = "http://something_or_other"
    mock_scheduler_client = Mock()
    mock_scheduler_client.scheduler.return_value = mock_scheduler
    mock_scheduler_client.url = "http://something_or_other"
    mock_api = Mock(spec=HookedAuroraClientAPI)
    mock_api.scheduler = mock_scheduler_client
    return (mock_api, mock_scheduler_client)

  @classmethod
  def create_mock_api_factory(cls):
    """Create a collection of mocks for a test that wants to mock out the client API
    by patching the api factory."""
    mock_api, mock_scheduler_client = cls.create_mock_api()
    mock_api_factory = Mock()
    mock_api_factory.return_value = mock_api
    return mock_api_factory, mock_scheduler_client

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
