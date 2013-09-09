import json
import tempfile
from textwrap import dedent

from twitter.aurora.client.api import AuroraClientAPI
from twitter.aurora.client.deployment_api import AuroraDeploymentAPI, DeploymentConfig
from twitter.aurora.common import AuroraJobKey, Cluster
from twitter.aurora.config import AuroraConfig, AuroraConfigLoader
from twitter.aurora.common_internal.packer_client import TwitterPacker
from twitter.packer import Packer

from gen.twitter.aurora.AuroraSchedulerManager import Client as scheduler_client
import gen.twitter.aurora.ttypes as ttypes

from .helper import FakeSchedulerProxy

import mox


class TestAuroraDeploymentAPI(mox.MoxTestBase):

  RAW_CONFIG = 'file_content'
  CLUSTER = Cluster(name='smfd')
  CONFIG_PACKAGE_NAME = "__job_smfd_devel_hello_world"
  JOB_KEY = AuroraJobKey(CLUSTER.name, 'johndoe', 'devel', 'hello_world')
  MESSAGE = "testing\\nmessages"
  METADATA = json.dumps({'message': MESSAGE})
  PROXY_HOST = 'nest1.corp.twitter.com'
  SESSION_KEY = 'asdf'
  DEPLOYMENT_CONFIG = DeploymentConfig(
      1, '5be07da9642fb3ec5bc0df0c1290dada',
      [{u'timestamp': 1374779340660,
        u'state': u'PRESENT',
        u'user': u'johndoe'}],
      METADATA, True)
  PACKER_DEPLOYMENT_CONFIG = {
      u'id': 1, u'md5sum': u'5be07da9642fb3ec5bc0df0c1290dada',
      u'uri': u'hftp://shortened', u'filename': u'job_description_eFA3CX',
      u'auditLog': [{
          u'timestamp': 1374779340660,
          u'state': u'PRESENT',
          u'user': u'johndoe'}],
      u'metadata': METADATA}

  CONFIG_CONTENT = dedent("""
    hello_world = Task(
      resources = Resources(cpu = 0.1, ram = 16 * MB, disk = 16 * MB),
      processes = [Process(
        name = 'hello_world',
        cmdline = 'echo hello world')])

    jobs = [Job(
        name = 'hello_world',
        environment = 'devel',
        cluster = 'smfd',
        role = 'johndoe',
        task = hello_world,
        instances = 1)]
  """)

  def setUp(self):
    super(TestAuroraDeploymentAPI, self).setUp()
    self.write_config_file()
    self.mock_packer = self.mox.CreateMock(TwitterPacker)
    self.mock_scheduler = self.mox.CreateMock(scheduler_client)
    self.api = AuroraClientAPI(self.CLUSTER)
    self.api._scheduler = FakeSchedulerProxy(self.CLUSTER, self.mock_scheduler, self.SESSION_KEY)
    self.deployment_api = AuroraDeploymentAPI(self.api, self.mock_packer)

  def write_config_file(self):
    self.config_file = tempfile.NamedTemporaryFile()
    self.config_file.write(self.CONFIG_CONTENT)
    self.config_file.seek(0)

    # Dump and reload config to simplify equality tests (otherwise attributes end up in different
    # orders)
    self.config = AuroraConfig.loads_json(AuroraConfig.load(
        self.config_file.name,
        self.JOB_KEY.name,
        None,
        select_cluster=self.JOB_KEY.cluster,
        select_env=self.JOB_KEY.env
    ).raw().json_dumps())

  def test_create(self):
    self.mock_packer.add(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME,
        mox.Func(self.is_deployment_file), self.METADATA)

    self.mox.ReplayAll()

    self.deployment_api.create(self.JOB_KEY, self.config_file.name, self.MESSAGE)

  def is_deployment_file(self, filename):
    with open(filename) as f:
      deployment = json.load(f)
    config = AuroraConfig.loads_json(deployment['job'])
    assert(config.job() == self.config.job())
    assert(deployment['loadables']['\x00' + self.config_file.name] == self.CONFIG_CONTENT)
    return True

  def test_create_no_job(self):
    job_key = AuroraJobKey(
        cluster=self.JOB_KEY.name,
        role=self.JOB_KEY.role,
        env=self.JOB_KEY.env,
        name='nope'
    )
    self.assertRaises(
        ValueError, self.deployment_api.create, job_key, self.config_file, self.MESSAGE)

  def test_log(self):
    self.mock_packer.list_versions(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME).AndReturn([self.PACKER_DEPLOYMENT_CONFIG])
    self.mock_packer.get_version(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME, 'latest').AndReturn(
            self.PACKER_DEPLOYMENT_CONFIG)
    self.mox.ReplayAll()

    assert self.deployment_api.log(self.JOB_KEY)[0].__dict__ == self.DEPLOYMENT_CONFIG.__dict__

  def test_log_no_deployment(self):
    self.mock_packer.list_versions(
        self.JOB_KEY.role,
        self.CONFIG_PACKAGE_NAME).AndRaise(Packer.Error('Requested package or version not found'))
    self.mox.ReplayAll()

    self.assertRaises(AuroraDeploymentAPI.NoDeploymentError, self.deployment_api.log, self.JOB_KEY)

  def mock_fetch(self, pkg, version, contents):
    def write_package_file(role, pkg, version, proxy_host, pkg_file):
      pkg_file.write(contents)
      pkg_file.flush()

    self.mock_packer.fetch(
        self.JOB_KEY.role, pkg, version,
        mox.IgnoreArg(), mox.IgnoreArg()).WithSideEffects(write_package_file)

  def mock_packer_release(self):
    config_pkg = self.CONFIG_PACKAGE_NAME
    self.mock_packer.get_version(
        self.JOB_KEY.role, config_pkg, 'latest').AndReturn(self.PACKER_DEPLOYMENT_CONFIG)
    self.mock_packer.get_version(
        self.JOB_KEY.role, config_pkg, 'latest').AndReturn(self.PACKER_DEPLOYMENT_CONFIG)
    self.mock_fetch(config_pkg, 'latest', json.dumps(
        {'loadables': AuroraConfigLoader(self.config_file.name).loadables,
          'job': self.config.raw().json_dumps()}))
    self.mock_packer.set_live(self.JOB_KEY.role, config_pkg, str(self.DEPLOYMENT_CONFIG.version_id))

  def test_release_new_job(self):
    self.mock_packer_release()
    response = ttypes.ScheduleStatusResponse(responseCode=ttypes.ResponseCode.INVALID_REQUEST)
    self.mock_scheduler.getTasksStatus(mox.IgnoreArg()).AndReturn(response)
    create_response = ttypes.Response(responseCode=ttypes.ResponseCode.OK)
    self.mock_scheduler.createJob(self.config.job(), self.SESSION_KEY).AndReturn(create_response)

    self.mox.ReplayAll()

    assert self.deployment_api.release(self.JOB_KEY, 15, self.PROXY_HOST) == create_response

  def test_release_existing_job(self):
    self.mock_packer_release()
    status_response = ttypes.ScheduleStatusResponse(
        responseCode=ttypes.ResponseCode.OK,
        tasks=[ttypes.ScheduledTask(status=ttypes.ScheduleStatus.RUNNING)])
    self.mock_scheduler.getTasksStatus(mox.IgnoreArg()).AndReturn(status_response)

    self.mox.StubOutWithMock(self.api, "update_job")
    update_response = ttypes.Response(responseCode=ttypes.ResponseCode.OK)
    self.api.update_job(
        mox.IgnoreArg(), health_check_interval_seconds=mox.IgnoreArg()).AndReturn(update_response)

    self.mox.ReplayAll()

    assert self.deployment_api.release(self.JOB_KEY, 15, self.PROXY_HOST) == update_response

  def test_reset(self):
    PACKER_CONTENT = json.dumps(
        {'job': self.config.raw().json_dumps(),
         'loadables': self.CONFIG_CONTENT})

    def write_file(_, _2, _3, _4, f):
      f.write(PACKER_CONTENT)

    def is_config_file(filename):
      with open(filename, 'r') as f:
        assert f.read() == PACKER_CONTENT
      return True

    reset_version = str(self.DEPLOYMENT_CONFIG.version_id)
    self.mock_packer.get_version(
        self.JOB_KEY.role,
        self.CONFIG_PACKAGE_NAME,
        'latest').AndReturn(self.PACKER_DEPLOYMENT_CONFIG)
    self.mock_packer.get_version(
        self.JOB_KEY.role,
        self.CONFIG_PACKAGE_NAME,
        reset_version).AndReturn(self.PACKER_DEPLOYMENT_CONFIG)
    self.mock_packer.fetch(
        self.JOB_KEY.role,
        self.CONFIG_PACKAGE_NAME,
        reset_version,
        self.PROXY_HOST,
        mox.IgnoreArg()).WithSideEffects(write_file)
    self.mock_packer.add(
        self.JOB_KEY.role,
        self.CONFIG_PACKAGE_NAME,
        mox.Func(is_config_file),
        self.METADATA)
    self.mox.ReplayAll()

    self.deployment_api.reset(self.JOB_KEY, reset_version, self.PROXY_HOST)

  def test_reset_no_deployment(self):
    reset_version = '15'
    self.mock_packer.get_version(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME, 'latest').AndRaise(
            Packer.Error('Requested package or version not found'))
    self.mock_packer.list_versions(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME).AndRaise(
            Packer.Error('Requested package or version not found'))
    self.mox.ReplayAll()

    self.assertRaises(
        AuroraDeploymentAPI.NoDeploymentError,
        self.deployment_api.reset,
        self.JOB_KEY,
        reset_version,
        self.PROXY_HOST)

  def test_reset_version_does_not_exist(self):
    reset_version = '15'
    self.mock_packer.get_version(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME, 'latest').AndReturn(
            self.PACKER_DEPLOYMENT_CONFIG)
    self.mock_packer.get_version(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME, '15').AndRaise(
            Packer.Error('Requested package or version not found'))
    self.mock_packer.list_versions(
        self.JOB_KEY.role, self.CONFIG_PACKAGE_NAME).AndReturn([self.PACKER_DEPLOYMENT_CONFIG])
    self.mox.ReplayAll()

    self.assertRaises(
        AuroraDeploymentAPI.NoSuchVersion,
        self.deployment_api.reset,
        self.JOB_KEY,
        reset_version,
        self.PROXY_HOST)
