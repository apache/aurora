import tempfile

from twitter.mesos.common import AuroraJobKey, Cluster
from twitter.mesos.client.stage_api import AuroraStageAPI
from twitter.mesos.client.stage_cli import AuroraStageCLI

import gen.twitter.mesos.ttypes as ttypes

import mox
from argparse import Namespace


class TestStageCLI(mox.MoxTestBase):
  JOB_KEY = "smfd/b/c/d"
  AURORA_JOB_KEY = AuroraJobKey.from_path(JOB_KEY)
  OPTIONS = Namespace(verbosity='normal')
  CLUSTERS = {'smfd': Cluster(name='smfd')}
  PROXY_HOST = 'nest1.corp.twitter.com'
  CREATE_RESPONSE = ttypes.CreateJobResponse(responseCode=ttypes.ResponseCode.OK)
  CHECK_INTERVAL = 1234

  def setUp(self):
    super(TestStageCLI, self).setUp()
    self.stage_api = self.mox.CreateMock(AuroraStageAPI)
    self.stage_cli = AuroraStageCLI(clusters=self.CLUSTERS, stage_api=self.stage_api)

  def test_dispatch_create(self):
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name]
      self.stage_api.create(self.AURORA_JOB_KEY, f.name)
      self.mox.ReplayAll()

      self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_create_release(self):
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name, '--release',
              '--updater_health_check_interval_seconds=' + str(self.CHECK_INTERVAL)]
      self.stage_api.create(self.AURORA_JOB_KEY, f.name)
      self.stage_api.release(
          self.AURORA_JOB_KEY, self.CHECK_INTERVAL, self.PROXY_HOST).AndReturn(self.CREATE_RESPONSE)
      self.mox.ReplayAll()

      self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_release(self):
    args = ['release', self.JOB_KEY,
            '--updater_health_check_interval_seconds=' + str(self.CHECK_INTERVAL)]
    self.stage_api.release(
        self.AURORA_JOB_KEY, self.CHECK_INTERVAL, self.PROXY_HOST).AndReturn(self.CREATE_RESPONSE)
    self.mox.ReplayAll()

    self.stage_cli.dispatch(args, self.OPTIONS)
