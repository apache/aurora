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

import threading

import pytest
from kazoo.client import KazooClient
from kazoo.exceptions import KazooException
from mock import MagicMock, create_autospec, patch
from twitter.common.quantity import Amount, Time
from twitter.common.testing.clock import ThreadedClock
from twitter.common.zookeeper.serverset import Endpoint, ServerSet

from apache.aurora.config.schema.base import HealthCheckConfig
from apache.aurora.executor.common.announcer import (
    Announcer,
    DefaultAnnouncerCheckerProvider,
    ServerSetJoinThread,
    make_endpoints
)


def test_serverset_join_thread():
  join = threading.Event()
  joined = threading.Event()

  def join_function():
    joined.set()

  ssjt = ServerSetJoinThread(join, join_function, loop_wait=Amount(1, Time.MILLISECONDS))
  ssjt.start()
  ssjt.stop()
  ssjt.join(timeout=1.0)
  assert not ssjt.is_alive()
  assert not joined.is_set()

  ssjt = ServerSetJoinThread(join, join_function, loop_wait=Amount(1, Time.MILLISECONDS))
  ssjt.start()

  def test_loop():
    join.set()
    joined.wait(timeout=1.0)
    assert not join.is_set()  # join is cleared
    assert joined.is_set()  # joined has been called
    joined.clear()

  # validate that the loop is working
  test_loop()
  test_loop()

  ssjt.stop()
  ssjt.join(timeout=1.0)
  assert not ssjt.is_alive()
  assert not joined.is_set()


def test_announcer_under_normal_circumstances():
  joined = threading.Event()

  def joined_side_effect(*args, **kw):
    joined.set()
    return 'membership foo'

  mock_serverset = create_autospec(spec=ServerSet, instance=True)
  mock_serverset.join = MagicMock()
  mock_serverset.join.side_effect = joined_side_effect
  mock_serverset.cancel = MagicMock()

  endpoint = Endpoint('localhost', 12345)
  clock = ThreadedClock(31337.0)

  announcer = Announcer(mock_serverset, endpoint, clock=clock)
  assert announcer.disconnected_time() == 0.0
  clock.tick(1.0)
  assert announcer.disconnected_time() == 1.0, (
      'Announcer should advance disconnection time when not yet initially connected.')

  announcer.start()

  try:
    joined.wait(timeout=1.0)
    assert joined.is_set()

    assert announcer.disconnected_time() == 0.0
    clock.tick(1.0)
    assert announcer.disconnected_time() == 0.0, (
        'Announcer should not advance disconnection time when connected.')
    assert announcer._membership == 'membership foo'

  finally:
    announcer.stop()

  mock_serverset.cancel.assert_called_with('membership foo')

  assert announcer.disconnected_time() == 0.0
  clock.tick(1.0)
  assert announcer.disconnected_time() == 0.0, (
      'Announcer should not advance disconnection time when stopped.')


@pytest.mark.skipif('True', reason='Flaky test (AURORA-639)')
def test_announcer_on_expiration():
  joined = threading.Event()
  operations = []

  def joined_side_effect(*args, **kw):
    # 'global' does not work within python nested functions, so we cannot use a
    # counter here, so instead we do append/len (see PEP-3104)
    operations.append(1)
    if len(operations) == 1 or len(operations) == 3:
      joined.set()
      return 'membership %d' % len(operations)
    else:
      raise KazooException('Failed to reconnect')

  mock_serverset = create_autospec(spec=ServerSet, instance=True)
  mock_serverset.join = MagicMock()
  mock_serverset.join.side_effect = joined_side_effect
  mock_serverset.cancel = MagicMock()

  endpoint = Endpoint('localhost', 12345)
  clock = ThreadedClock(31337.0)

  announcer = Announcer(
      mock_serverset, endpoint, clock=clock, exception_wait=Amount(2, Time.SECONDS))
  announcer.start()

  try:
    joined.wait(timeout=1.0)
    assert joined.is_set()
    assert announcer._membership == 'membership 1'
    assert announcer.disconnected_time() == 0.0
    clock.tick(1.0)
    assert announcer.disconnected_time() == 0.0
    announcer.on_expiration()  # expect exception
    clock.tick(1.0)
    assert announcer.disconnected_time() == 1.0, (
        'Announcer should be disconnected on expiration.')
    clock.tick(10.0)
    assert announcer.disconnected_time() == 0.0, (
        'Announcer should not advance disconnection time when connected.')
    assert announcer._membership == 'membership 3'

  finally:
    announcer.stop()


@pytest.mark.skipif('True', reason='Flaky test (AURORA-639)')
def test_announcer_under_abnormal_circumstances():
  mock_serverset = create_autospec(spec=ServerSet, instance=True)
  mock_serverset.join = MagicMock()
  mock_serverset.join.side_effect = [
      KazooException('Whoops the ensemble is down!'),
      'member0001',
  ]
  mock_serverset.cancel = MagicMock()

  endpoint = Endpoint('localhost', 12345)
  clock = ThreadedClock(31337.0)

  announcer = Announcer(
       mock_serverset, endpoint, clock=clock, exception_wait=Amount(2, Time.SECONDS))
  announcer.start()

  try:
    clock.tick(1.0)
    assert announcer.disconnected_time() == 1.0
    clock.tick(2.0)
    assert announcer.disconnected_time() == 0.0, (
        'Announcer should recover after an exception thrown internally.')
    assert announcer._membership == 'member0001'
  finally:
    announcer.stop()


def make_assigned_task(thermos_config, assigned_ports=None):
  from gen.apache.aurora.api.constants import AURORA_EXECUTOR_NAME
  from gen.apache.aurora.api.ttypes import (
      AssignedTask,
      ExecutorConfig,
      JobKey,
      TaskConfig
  )

  assigned_ports = assigned_ports or {}
  executor_config = ExecutorConfig(name=AURORA_EXECUTOR_NAME, data=thermos_config.json_dumps())
  task_config = TaskConfig(
      job=JobKey(
          role=thermos_config.role().get(),
          environment="prod",
          name=thermos_config.name().get()),
      executorConfig=executor_config)

  return AssignedTask(
      instanceId=12345,
      task=task_config,
      assignedPorts=assigned_ports,
      slaveHost='test-host')


def make_job(role, environment, name, primary_port, portmap, zk_path=None):
  from apache.aurora.config.schema.base import (
      Announcer,
      Job,
      Process,
      Resources,
      Task,
  )
  task = Task(
      name='ignore2',
      processes=[Process(name='ignore3', cmdline='ignore4')],
      resources=Resources(cpu=1, ram=1, disk=1))
  if zk_path:
    announce = Announcer(primary_port=primary_port, portmap=portmap, zk_path=zk_path)
  else:
    announce = Announcer(primary_port=primary_port, portmap=portmap)
  job = Job(
      role=role,
      environment=environment,
      name=name,
      cluster='ignore1',
      task=task,
      announce=announce)
  return job


def test_make_empty_endpoints():
  hostname = 'aurora.example.com'
  portmap = {}
  primary_port = 'http'

  # test no bound 'http' port
  primary, additional = make_endpoints(hostname, portmap, primary_port)
  assert primary == Endpoint(hostname, 0)
  assert additional == {}


@patch('apache.aurora.executor.common.announcer.ServerSet')
@patch('apache.aurora.executor.common.announcer.KazooClient')
def test_announcer_provider_with_timeout(mock_client_provider, mock_serverset_provider):
  mock_client = create_autospec(spec=KazooClient, instance=True)
  mock_client_provider.return_value = mock_client
  client_connect_event = threading.Event()
  mock_client.start_async.return_value = client_connect_event

  mock_serverset = create_autospec(spec=ServerSet, instance=True)
  mock_serverset_provider.return_value = mock_serverset

  dap = DefaultAnnouncerCheckerProvider('zookeeper.example.com', root='/aurora')
  job = make_job('aurora', 'prod', 'proxy', 'primary', portmap={'http': 80, 'admin': 'primary'})

  health_check_config = HealthCheckConfig(initial_interval_secs=0.1, interval_secs=0.1)
  job = job(health_check_config=health_check_config)
  assigned_task = make_assigned_task(job, assigned_ports={'primary': 12345})
  checker = dap.from_assigned_task(assigned_task, None)

  mock_client.start_async.assert_called_once_with()
  mock_serverset_provider.assert_called_once_with(mock_client, '/aurora/aurora/prod/proxy')

  checker.start()
  checker.start_event.wait()

  assert checker.status is not None


@patch('apache.aurora.executor.common.announcer.ServerSet')
@patch('apache.aurora.executor.common.announcer.KazooClient')
def test_default_announcer_provider(mock_client_provider, mock_serverset_provider):
  mock_client = create_autospec(spec=KazooClient, instance=True)
  mock_client_provider.return_value = mock_client
  mock_serverset = create_autospec(spec=ServerSet, instance=True)
  mock_serverset_provider.return_value = mock_serverset

  dap = DefaultAnnouncerCheckerProvider('zookeeper.example.com', root='/aurora')
  job = make_job('aurora', 'prod', 'proxy', 'primary', portmap={'http': 80, 'admin': 'primary'})
  assigned_task = make_assigned_task(job, assigned_ports={'primary': 12345})
  checker = dap.from_assigned_task(assigned_task, None)

  mock_client.start_async.assert_called_once_with()
  mock_serverset_provider.assert_called_once_with(mock_client, '/aurora/aurora/prod/proxy')
  assert checker.name() == 'announcer'
  assert checker.status is None


def test_default_announcer_provider_without_announce():
  from pystachio import Empty

  job = make_job('aurora', 'prod', 'proxy', 'primary', portmap={})
  job = job(announce=Empty)
  assigned_task = make_assigned_task(job)

  assert DefaultAnnouncerCheckerProvider('foo.bar').from_assigned_task(assigned_task, None) is None


@patch('apache.aurora.executor.common.announcer.ServerSet')
@patch('apache.aurora.executor.common.announcer.KazooClient')
def test_announcer_provider_with_zkpath(mock_client_provider, mock_serverset_provider):
  mock_client = create_autospec(spec=KazooClient, instance=True)
  mock_client_provider.return_value = mock_client
  mock_serverset = create_autospec(spec=ServerSet, instance=True)
  mock_serverset_provider.return_value = mock_serverset

  dap = DefaultAnnouncerCheckerProvider('zookeeper.example.com', '', True)
  job = make_job('aurora', 'prod', 'proxy', 'primary', portmap={'http': 80, 'admin': 'primary'},
   zk_path='/uns/v1/sjc1-prod/us1/service/prod')
  assigned_task = make_assigned_task(job, assigned_ports={'primary': 12345})
  checker = dap.from_assigned_task(assigned_task, None)

  mock_client.start_async.assert_called_once_with()
  mock_serverset_provider.assert_called_once_with(mock_client, '/uns/v1/sjc1-prod/us1/service/prod')
  assert checker.name() == 'announcer'
  assert checker.status is None
