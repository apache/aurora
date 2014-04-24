#
# Copyright 2013 Apache Software Foundation
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

import inspect
import unittest

from apache.aurora.common.cluster import Cluster
import apache.aurora.client.api.scheduler_client as scheduler_client
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.kazoo_client import TwitterKazooClient
from twitter.common.zookeeper.serverset.endpoint import ServiceInstance

import gen.apache.aurora.api.AuroraAdmin as AuroraAdmin
import gen.apache.aurora.api.AuroraSchedulerManager as AuroraSchedulerManager
from gen.apache.aurora.api.constants import DEFAULT_ENVIRONMENT, CURRENT_API_VERSION
from gen.apache.aurora.api.ttypes import *

from mox import IgnoreArg, IsA, Mox


ROLE = 'foorole'
JOB_NAME = 'barjobname'
JOB_KEY = JobKey(role=ROLE, environment=DEFAULT_ENVIRONMENT, name=JOB_NAME)


def test_testCoverage():
  """Make sure a new thrift RPC doesn't get added without minimal test coverage."""
  for name, klass in inspect.getmembers(AuroraAdmin) + inspect.getmembers(AuroraSchedulerManager):
    if name.endswith('_args'):
      rpc_name = name[:-len('_args')]
      assert hasattr(TestSchedulerProxyAdminInjection, 'test_%s' % rpc_name), (
              'No test defined for RPC %s' % rpc_name)


class TestSchedulerProxy(scheduler_client.SchedulerProxy):
  """In testing we shouldn't use the real SSHAgentAuthenticator."""
  def session_key(self):
    return self.create_session('SOME_USER')

  @classmethod
  def create_session(cls, user):
    return SessionKey(mechanism='test', data='test')


class TestSchedulerProxyInjection(unittest.TestCase):
  def setUp(self):
    self.mox = Mox()

    self.mox.StubOutClassWithMocks(AuroraAdmin, 'Client')
    self.mox.StubOutClassWithMocks(scheduler_client, 'SchedulerClient')

    self.mock_scheduler_client = self.mox.CreateMock(scheduler_client.SchedulerClient)
    self.mock_thrift_client = self.mox.CreateMock(AuroraAdmin.Client)

    scheduler_client.SchedulerClient.get(IgnoreArg(), verbose=IgnoreArg()).AndReturn(
        self.mock_scheduler_client)
    self.mock_scheduler_client.get_thrift_client().AndReturn(self.mock_thrift_client)

    version_resp = Response(responseCode=ResponseCode.OK)
    version_resp.result = Result(getVersionResult = CURRENT_API_VERSION)

    self.mock_thrift_client.getVersion().AndReturn(version_resp)

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.VerifyAll()

  def make_scheduler_proxy(self):
    return TestSchedulerProxy('local')

  def test_startCronJob(self):
    self.mock_thrift_client.startCronJob(IsA(JobKey), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().startCronJob(JOB_KEY)

  def test_createJob(self):
    self.mock_thrift_client.createJob(IsA(JobConfiguration), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().createJob(JobConfiguration())

  def test_replaceCronTemplate(self):
    self.mock_thrift_client.replaceCronTemplate(IsA(JobConfiguration), IsA(Lock), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().replaceCronTemplate(JobConfiguration(), Lock())

  def test_populateJobConfig(self):
    self.mock_thrift_client.populateJobConfig(IsA(JobConfiguration))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().populateJobConfig(JobConfiguration())

  def test_restartShards(self):
    self.mock_thrift_client.restartShards(IsA(JobKey), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().restartShards(JOB_KEY, set([0]))

  def test_getTasksStatus(self):
    self.mock_thrift_client.getTasksStatus(IsA(TaskQuery))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().getTasksStatus(TaskQuery())

  def test_getJobs(self):
    self.mock_thrift_client.getJobs(IgnoreArg())

    self.mox.ReplayAll()

    self.make_scheduler_proxy().getJobs(ROLE)

  def test_killTasks(self):
    self.mock_thrift_client.killTasks(IsA(TaskQuery), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().killTasks(TaskQuery())

  def test_getQuota(self):
    self.mock_thrift_client.getQuota(IgnoreArg())

    self.mox.ReplayAll()

    self.make_scheduler_proxy().getQuota(ROLE)

  def test_startMaintenance(self):
    self.mock_thrift_client.startMaintenance(IsA(Hosts), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().startMaintenance(Hosts())

  def test_drainHosts(self):
    self.mock_thrift_client.drainHosts(IsA(Hosts), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().drainHosts(Hosts())

  def test_maintenanceStatus(self):
    self.mock_thrift_client.maintenanceStatus(IsA(Hosts), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().maintenanceStatus(Hosts())

  def test_endMaintenance(self):
    self.mock_thrift_client.endMaintenance(IsA(Hosts), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().endMaintenance(Hosts())

  def test_getVersion(self):
    self.mock_thrift_client.getVersion()

    self.mox.ReplayAll()

    self.make_scheduler_proxy().getVersion()

  def test_addInstances(self):
    self.mock_thrift_client.addInstances(IsA(JobKey), IgnoreArg(), IsA(Lock), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().addInstances(JobKey(), {}, Lock())

  def test_acquireLock(self):
    self.mock_thrift_client.acquireLock(IsA(Lock), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().acquireLock(Lock())

  def test_releaseLock(self):
    self.mock_thrift_client.releaseLock(IsA(Lock), IsA(LockValidation), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().releaseLock(Lock(), LockValidation())


class TestSchedulerProxyAdminInjection(TestSchedulerProxyInjection):
  def test_setQuota(self):
    self.mock_thrift_client.setQuota(IgnoreArg(), IsA(ResourceAggregate), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().setQuota(ROLE, ResourceAggregate())

  def test_forceTaskState(self):
    self.mock_thrift_client.forceTaskState(IgnoreArg(), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().forceTaskState('taskid', ScheduleStatus.LOST)

  def test_performBackup(self):
    self.mock_thrift_client.performBackup(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().performBackup()

  def test_listBackups(self):
    self.mock_thrift_client.listBackups(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().listBackups()

  def test_stageRecovery(self):
    self.mock_thrift_client.stageRecovery(IsA(TaskQuery), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().stageRecovery(TaskQuery())

  def test_queryRecovery(self):
    self.mock_thrift_client.queryRecovery(IsA(TaskQuery), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().queryRecovery(TaskQuery())

  def test_deleteRecoveryTasks(self):
    self.mock_thrift_client.deleteRecoveryTasks(IsA(TaskQuery), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().deleteRecoveryTasks(TaskQuery())

  def test_commitRecovery(self):
    self.mock_thrift_client.commitRecovery(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().commitRecovery()

  def test_unloadRecovery(self):
    self.mock_thrift_client.unloadRecovery(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().unloadRecovery()

  def test_snapshot(self):
    self.mock_thrift_client.snapshot(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().snapshot()

  def test_rewriteConfigs(self):
    self.mock_thrift_client.rewriteConfigs(IsA(RewriteConfigsRequest), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().rewriteConfigs(RewriteConfigsRequest())


class TestZookeeperSchedulerClient(unittest.TestCase):
  def setUp(self):
    self.mox = Mox()

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.VerifyAll()

  def test_url_when_not_connected_and_cluster_has_no_proxy_url(self):
    host = 'some-host.example.com'
    port = 31181

    mock_zk = self.mox.CreateMock(TwitterKazooClient)

    def mock_get_serverset(*args, **kwargs):
      service_json = '''{
        "additionalEndpoints": {
            "http": {
                "host": "%s",
                "port": %d
            }
        },
        "serviceEndpoint": {
            "host": "%s",
            "port": %d
        },
        "shard": 0,
        "status": "ALIVE"
    }''' % (host, port, host, port)

      return mock_zk, [ServiceInstance.unpack(service_json)]

    class TestZookeeperSchedulerClient(scheduler_client.ZookeeperSchedulerClient):
      SERVERSET_TIMEOUT = Amount(10, Time.MILLISECONDS)

    original_method = TestZookeeperSchedulerClient.get_scheduler_serverset

    try:
      TestZookeeperSchedulerClient.get_scheduler_serverset = mock_get_serverset

      zk_scheduler_client = TestZookeeperSchedulerClient(Cluster(proxy_url=None))
      self.mox.StubOutWithMock(zk_scheduler_client, '_connect_scheduler')
      mock_zk.stop()
      zk_scheduler_client._connect_scheduler(host, port, False)

      self.mox.ReplayAll()

      assert zk_scheduler_client.url == 'http://%s:%d' % (host, port)
    finally:
      TestZookeeperSchedulerClient.get_scheduler_serverset = original_method
