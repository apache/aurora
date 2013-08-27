from datetime import datetime
import json
import tempfile

from twitter.aurora.common import AuroraJobKey, Cluster
from twitter.aurora.client.deployment_api import AuroraDeploymentAPI, DeploymentConfig
from twitter.aurora.client.deployment_cli import AuroraDeploymentCLI, DeploymentConfigFormat

import gen.twitter.aurora.ttypes as ttypes

import mox
from argparse import Namespace


class TestDeploymentCLI(mox.MoxTestBase):
  JOB_KEY = "smfd/b/c/d"
  AURORA_JOB_KEY = AuroraJobKey.from_path(JOB_KEY)
  OPTIONS = Namespace(verbosity='normal')
  CLUSTERS = {'smfd': Cluster(name='smfd')}
  PROXY_HOST = 'nest1.corp.twitter.com'
  CREATE_RESPONSE = ttypes.CreateJobResponse(responseCode=ttypes.ResponseCode.OK)
  CHECK_INTERVAL = 1234

  DEPLOYMENT_CONFIG_CREATE_TIMESTAMP = 1374779340660
  DEPLOYMENT_CONFIG_RELEASE_TIMESTAMP = 1377779340660
  DEPLOYMENT_CONFIG = DeploymentConfig(
      '1', '5be07da9642fb3ec5bc0df0c1290dada',
      [{u'timestamp': DEPLOYMENT_CONFIG_CREATE_TIMESTAMP,
        u'state': u'PRESENT',
        u'user': u'johndoe'},
       {u'timestamp': DEPLOYMENT_CONFIG_RELEASE_TIMESTAMP,
        u'state': u'LIVE',
        u'user': u'jane'}],
      json.dumps({"message": "testing\nmessages"}), True)
  RAW_CONFIG = """config
content
"""
  SCHEDULED_JOB = json.dumps({})
  DEPLOYMENT_CONFIG_CONTENT = {"loadables": {"myfile.mesos": RAW_CONFIG}, "job": SCHEDULED_JOB}

  def setUp(self):
    super(TestDeploymentCLI, self).setUp()
    self.deployment_api = self.mox.CreateMock(AuroraDeploymentAPI)
    self.deployment_cli = AuroraDeploymentCLI(clusters=self.CLUSTERS, deployment_api=self.deployment_api)

  def test_dispatch_create(self):
    message = "test message"
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name, '-m' + message]
      self.deployment_api.create(self.AURORA_JOB_KEY, f.name, message)
      self.mox.ReplayAll()

      self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_create_no_message(self):
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name]
      self.deployment_api.create(self.AURORA_JOB_KEY, f.name, None)
      self.mox.ReplayAll()

      self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_create_release(self):
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name, '--release',
              '--updater_health_check_interval_seconds=' + str(self.CHECK_INTERVAL)]
      self.deployment_api.create(self.AURORA_JOB_KEY, f.name, None)
      self.deployment_api.release(
          self.AURORA_JOB_KEY, self.CHECK_INTERVAL, self.PROXY_HOST).AndReturn(self.CREATE_RESPONSE)
      self.mox.ReplayAll()

      self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_log(self):
    args = ['log', self.JOB_KEY]

    self.deployment_api.log(self.AURORA_JOB_KEY).AndReturn([self.DEPLOYMENT_CONFIG])
    self.mox.ReplayAll()

    self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_log_long(self):
    args = ['log', self.JOB_KEY, '--long']

    self.deployment_api.log(self.AURORA_JOB_KEY).AndReturn([self.DEPLOYMENT_CONFIG])
    self.mox.ReplayAll()

    self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_release(self):
    args = ['release', self.JOB_KEY,
            '--updater_health_check_interval_seconds=' + str(self.CHECK_INTERVAL)]
    self.deployment_api.release(
        self.AURORA_JOB_KEY, self.CHECK_INTERVAL, self.PROXY_HOST).AndReturn(self.CREATE_RESPONSE)
    self.mox.ReplayAll()

    self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_reset(self):
    version_id = '14'
    args = ['reset', self.JOB_KEY, version_id]

    self.deployment_api.reset(self.AURORA_JOB_KEY, int(version_id), self.PROXY_HOST)
    self.mox.ReplayAll()

    self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_show(self):
    args = ['show', self.JOB_KEY]

    self.deployment_api.show(self.AURORA_JOB_KEY, 'latest', self.PROXY_HOST).AndReturn(
        [self.DEPLOYMENT_CONFIG, self.DEPLOYMENT_CONFIG_CONTENT])
    self.mox.ReplayAll()

    self.deployment_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_show_version(self):
    version_id = '14'
    args = ['show', self.JOB_KEY, version_id]

    self.deployment_api.show(self.AURORA_JOB_KEY, version_id, self.PROXY_HOST).AndReturn(
        [self.DEPLOYMENT_CONFIG, self.DEPLOYMENT_CONFIG_CONTENT])
    self.mox.ReplayAll()

    self.deployment_cli.dispatch(args, self.OPTIONS)

  LONG_LOG = """Version: 1 (md5: 5be07da9642fb3ec5bc0df0c1290dada) (Currently released)
Created by: johndoe
Date created: %s
Status: RELEASED
Released by: jane
Date released: %s

    testing
    messages
""" % (datetime.fromtimestamp(DEPLOYMENT_CONFIG_CREATE_TIMESTAMP / 1000).isoformat(' '),
       datetime.fromtimestamp(DEPLOYMENT_CONFIG_RELEASE_TIMESTAMP / 1000).isoformat(' '))

  def test_format_full(self):
    expected = self.LONG_LOG + """
Scheduled job:
--
%s
--
Raw file: myfile.mesos
%s""" % (self.SCHEDULED_JOB, self.RAW_CONFIG)

    assert DeploymentConfigFormat.full_str(
        self.DEPLOYMENT_CONFIG, self.DEPLOYMENT_CONFIG_CONTENT) == expected

  def test_format_long(self):
    assert DeploymentConfigFormat.long_str(self.DEPLOYMENT_CONFIG) == self.LONG_LOG

  def test_format_one_line(self):
    log = "1 %s johndoe RELEASED testing" % datetime.fromtimestamp(
        self.DEPLOYMENT_CONFIG_CREATE_TIMESTAMP / 1000).isoformat(' ')

    assert DeploymentConfigFormat.one_line_str(self.DEPLOYMENT_CONFIG) == log
