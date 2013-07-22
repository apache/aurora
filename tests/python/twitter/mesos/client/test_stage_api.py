import json
import tempfile

from twitter.mesos.client.api import MesosClientAPI
from twitter.mesos.client.stage_api import AuroraStageAPI
from twitter.mesos.common import AuroraJobKey, Cluster
from twitter.mesos.config import AuroraConfig, AuroraConfigLoader
from twitter.mesos.packer.packer_client import Packer

from gen.twitter.mesos.MesosSchedulerManager import Client as scheduler_client
import gen.twitter.mesos.ttypes as ttypes

from .helper import FakeSchedulerProxy

import mox


RAW_CONFIG = 'file_content'
CLUSTER = Cluster(name='smfd')
JOB_KEY = AuroraJobKey(CLUSTER.name, 'johndoe', 'devel', 'hello_world')
PROXY_HOST = 'proxy'
SESSION_KEY = 'asdf'


config_content = """
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
"""


class TestAuroraStageAPI(mox.MoxTestBase):

  def setUp(self):
    super(TestAuroraStageAPI, self).setUp()
    self.write_config_file()
    self.mock_packer = self.mox.CreateMock(Packer)
    self.mock_scheduler = self.mox.CreateMock(scheduler_client)
    self.api = MesosClientAPI(CLUSTER)
    self.api._scheduler = FakeSchedulerProxy(CLUSTER, self.mock_scheduler, SESSION_KEY)
    self.stage_api = AuroraStageAPI(self.api, self.mock_packer)

  def write_config_file(self):
    self.config_file = tempfile.NamedTemporaryFile()
    self.config_file.write(config_content)
    self.config_file.seek(0)

    # Dump and reload config to simplify equality tests (otherwise attributes end up in different
    # orders)
    self.config = AuroraConfig.loads_json(AuroraConfig.load(
        self.config_file.name,
        JOB_KEY.name,
        None,
        select_cluster=JOB_KEY.cluster,
        select_env=JOB_KEY.env
    ).raw().json_dumps())

  def test_stage(self):
    self.mock_packer.add(
        JOB_KEY.role, self.stage_api._config_package_name(JOB_KEY),
        mox.Func(self.is_stage_file), {})

    self.mox.ReplayAll()

    self.stage_api.stage(JOB_KEY, self.config_file.name)

  def is_stage_file(self, filename):
    with open(filename) as f:
      staged = json.load(f)
    config = AuroraConfig.loads_json(staged['job'])
    assert(config.job() == self.config.job())
    assert(staged['loadables']['\x00' + self.config_file.name] == config_content)
    return True

  def test_stage_no_job(self):
    job_key = AuroraJobKey(
        cluster=JOB_KEY.name,
        role=JOB_KEY.role,
        env=JOB_KEY.env,
        name='nope'
    )
    self.assertRaises(
        ValueError,
        self.stage_api.stage,
        job_key,
        self.config_file.name)

  def mock_fetch(self, pkg, version, contents):
    def write_package_file(role, pkg, version, proxy_host, pkg_file):
      pkg_file.write(contents)
      pkg_file.flush()

    self.mock_packer.fetch(
        JOB_KEY.role, pkg, version, PROXY_HOST, mox.IgnoreArg()).WithSideEffects(
            write_package_file)

  def mock_packer_release(self):
    config_pkg = self.stage_api._config_package_name(JOB_KEY)
    latest_version = 1234
    self.mock_packer.get_version(
        JOB_KEY.role, config_pkg, 'latest').AndReturn({'id': latest_version})
    self.mock_fetch(config_pkg, str(latest_version), json.dumps(
        {'loadables': AuroraConfigLoader(self.config_file.name).loadables,
          'job': self.config.raw().json_dumps()}))
    self.mock_packer.set_live(JOB_KEY.role, config_pkg, str(latest_version))

  def test_release_new_job(self):
    self.mock_packer_release()
    response = ttypes.ScheduleStatusResponse(responseCode=ttypes.ResponseCode.INVALID_REQUEST)
    self.mock_scheduler.getTasksStatus(mox.IgnoreArg()).AndReturn(response)
    self.mock_scheduler.createJob(self.config.job(), SESSION_KEY)

    self.mox.ReplayAll()

    self.stage_api.release(JOB_KEY, 15, PROXY_HOST)

  def test_release_existing_job(self):
    self.mock_packer_release()
    response = ttypes.ScheduleStatusResponse(
        responseCode=ttypes.ResponseCode.OK,
        tasks=[ttypes.ScheduledTask(status=ttypes.ScheduleStatus.RUNNING)])
    self.mock_scheduler.getTasksStatus(mox.IgnoreArg()).AndReturn(response)
    self.mox.StubOutWithMock(self.api, "update_job")
    self.api.update_job(mox.IgnoreArg(), health_check_interval_seconds=mox.IgnoreArg())

    self.mox.ReplayAll()

    self.stage_api.release(JOB_KEY, 15, PROXY_HOST)
