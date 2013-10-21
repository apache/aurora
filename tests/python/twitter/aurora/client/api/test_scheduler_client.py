import inspect
import unittest

import twitter.aurora.client.api.scheduler_client as scheduler_client

import gen.twitter.aurora.AuroraAdmin as AuroraAdmin
import gen.twitter.aurora.AuroraSchedulerManager as AuroraSchedulerManager
from gen.twitter.aurora.constants import DEFAULT_ENVIRONMENT, CURRENT_API_VERSION
from gen.twitter.aurora.ttypes import *

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

  def test_startUpdate(self):
    self.mock_thrift_client.startUpdate(IsA(JobConfiguration), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().startUpdate(JobConfiguration())

  def test_updateShards(self):
    self.mock_thrift_client.updateShards(IsA(JobKey), IgnoreArg(), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().updateShards(JOB_KEY, set([0]), 'tok')

  def test_rollbackShards(self):
    self.mock_thrift_client.rollbackShards(IsA(JobKey), IgnoreArg(), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().rollbackShards(JOB_KEY, set([0]), 'tok')

  def test_finishUpdate(self):
    self.mock_thrift_client.finishUpdate(
        IsA(JobKey), IsA(UpdateResult), IgnoreArg(), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().finishUpdate(JOB_KEY, UpdateResult(), 'tok')

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

  def test_getJobUpdates(self):
    self.mock_thrift_client.getJobUpdates(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().getJobUpdates()

  def test_snapshot(self):
    self.mock_thrift_client.snapshot(IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().snapshot()

  def test_rewriteConfigs(self):
    self.mock_thrift_client.rewriteConfigs(IsA(RewriteConfigsRequest), IsA(SessionKey))

    self.mox.ReplayAll()

    self.make_scheduler_proxy().rewriteConfigs(RewriteConfigsRequest())
