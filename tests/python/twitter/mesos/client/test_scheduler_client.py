import inspect
import unittest

from mox import IsA, IgnoreArg, Mox
from thrift.protocol import TBinaryProtocol

import twitter.mesos.client.scheduler_client as scheduler_client

import gen.twitter.mesos.MesosAdmin as MesosAdmin
import gen.twitter.mesos.MesosSchedulerManager as MesosSchedulerManager
from gen.twitter.mesos.constants import DEFAULT_ENVIRONMENT
from gen.twitter.mesos.ttypes import *

ROLE = 'foorole'
JOB_NAME = 'barjobname'
JOB_KEY = JobKey(role=ROLE, environment=DEFAULT_ENVIRONMENT, name=JOB_NAME)

def test_testCoverage():
  """Make sure a new thrift RPC doesn't get added without minimal test coverage."""
  for name, klass in inspect.getmembers(MesosAdmin) + inspect.getmembers(MesosSchedulerManager):
    if name.endswith('_args'):
      rpc_name = name.strip('_args')
      assert (hasattr(TestSchedulerProxyAdminInjection, 'test_%s' % rpc_name),
              'No test defined for RPC %s' % rpc_name)

class TestSchedulerProxy(scheduler_client.SchedulerProxy):
  """In testing we shouldn't use the real SSHAgentAuthenticator."""
  @classmethod
  def create_session(cls, user):
    return SessionKey(user=user, nonce=42, nonceSig='UNAUTHENTICATED')

class TestSchedulerProxyInjection(unittest.TestCase):

  def setUp(self):
    self.mox = Mox()

    self.mox.StubOutClassWithMocks(MesosAdmin, 'Client')
    self.mox.StubOutClassWithMocks(scheduler_client, 'SchedulerClient')

    self.mock_scheduler_client = self.mox.CreateMock(scheduler_client.SchedulerClient)
    self.mock_thrift_client = self.mox.CreateMock(MesosAdmin.Client)

    scheduler_client.SchedulerClient.get(IgnoreArg(), verbose=IgnoreArg()).AndReturn(
        self.mock_scheduler_client)
    self.mock_scheduler_client.get_thrift_client().AndReturn(self.mock_thrift_client)

  def tearDown(self):
    self.mox.UnsetStubs()
    self.mox.VerifyAll()

  def make_scheduler_proxy(self):
    return TestSchedulerProxy('local')

  def test_startCronJob(self):
    self.mock_thrift_client.startCronJob(IgnoreArg(), IgnoreArg(), IsA(JobKey), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().startCronJob(
        'foo', 'bar', JobKey(role='foo', environment=DEFAULT_ENVIRONMENT, name='bar'))

  def test_createJob(self):
    self.mock_thrift_client.createJob(IsA(JobConfiguration), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().createJob(JobConfiguration())

  def test_populateJobConfig(self):
    self.mock_thrift_client.populateJobConfig(IsA(JobConfiguration))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().populateJobConfig(JobConfiguration())

  def test_startUpdate(self):
    self.mock_thrift_client.startUpdate(IsA(JobConfiguration), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().startUpdate(JobConfiguration())

  def test_updateShards(self):
    self.mock_thrift_client.updateShards(
        IgnoreArg(), IgnoreArg(), IsA(JobKey), IgnoreArg(), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().updateShards(ROLE, JOB_NAME, JOB_KEY, set([0]), 'tok')

  def test_rollbackShards(self):
    self.mock_thrift_client.rollbackShards(
        IgnoreArg(), IgnoreArg(), IsA(JobKey), IgnoreArg(), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().rollbackShards(ROLE, JOB_NAME, JOB_KEY, set([0]), 'tok')

  def test_finishUpdate(self):
    self.mock_thrift_client.finishUpdate(
        IgnoreArg(), IgnoreArg(), IsA(JobKey), IsA(UpdateResult), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().finishUpdate(ROLE, JOB_NAME, JOB_KEY, UpdateResult(), 'tok')

  def test_restartShards(self):
    self.mock_thrift_client.restartShards(
        IgnoreArg(), IgnoreArg(), IsA(JobKey), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().restartShards('foo', 'bar', JOB_KEY, set([0]))

  def test_getTasksStatus(self):
    self.mock_thrift_client.getTasksStatus(IsA(TaskQuery))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().getTasksStatus(TaskQuery())

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

class TestSchedulerProxyAdminInjection(TestSchedulerProxyInjection):
  def test_setQuota(self):
    self.mock_thrift_client.setQuota(IgnoreArg(), IsA(Quota), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().setQuota(ROLE, Quota())

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