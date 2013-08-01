from datetime import datetime
import json
import tempfile

from twitter.mesos.common import AuroraJobKey, Cluster
from twitter.mesos.client.stage_api import AuroraStageAPI, StagedConfig
from twitter.mesos.client.stage_cli import AuroraStageCLI, StagedConfigFormat

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

  STAGED_CONFIG_CREATE_TIMESTAMP = 1374779340660
  STAGED_CONFIG_RELEASE_TIMESTAMP = 1377779340660
  STAGED_CONFIG = StagedConfig(
      '1', '5be07da9642fb3ec5bc0df0c1290dada',
      [{u'timestamp': STAGED_CONFIG_CREATE_TIMESTAMP,
        u'state': u'PRESENT',
        u'user': u'johndoe'},
       {u'timestamp': STAGED_CONFIG_RELEASE_TIMESTAMP,
        u'state': u'LIVE',
        u'user': u'jane'}],
      json.dumps({"message": "testing\nmessages"}))

  def setUp(self):
    super(TestStageCLI, self).setUp()
    self.stage_api = self.mox.CreateMock(AuroraStageAPI)
    self.stage_cli = AuroraStageCLI(clusters=self.CLUSTERS, stage_api=self.stage_api)

  def test_dispatch_create(self):
    message = "test message"
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name, '-m' + message]
      self.stage_api.create(self.AURORA_JOB_KEY, f.name, message)
      self.mox.ReplayAll()

      self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_create_no_message(self):
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name]
      self.stage_api.create(self.AURORA_JOB_KEY, f.name, None)
      self.mox.ReplayAll()

      self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_create_release(self):
    with tempfile.NamedTemporaryFile() as f:
      args = ['create', self.JOB_KEY, f.name, '--release',
              '--updater_health_check_interval_seconds=' + str(self.CHECK_INTERVAL)]
      self.stage_api.create(self.AURORA_JOB_KEY, f.name, None)
      self.stage_api.release(
          self.AURORA_JOB_KEY, self.CHECK_INTERVAL, self.PROXY_HOST).AndReturn(self.CREATE_RESPONSE)
      self.mox.ReplayAll()

      self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_log(self):
    args = ['log', self.JOB_KEY]

    self.stage_api.log(self.AURORA_JOB_KEY).AndReturn([self.STAGED_CONFIG])
    self.mox.ReplayAll()

    self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_log_long(self):
    args = ['log', self.JOB_KEY, '--long']

    self.stage_api.log(self.AURORA_JOB_KEY).AndReturn([self.STAGED_CONFIG])
    self.mox.ReplayAll()

    self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_release(self):
    args = ['release', self.JOB_KEY,
            '--updater_health_check_interval_seconds=' + str(self.CHECK_INTERVAL)]
    self.stage_api.release(
        self.AURORA_JOB_KEY, self.CHECK_INTERVAL, self.PROXY_HOST).AndReturn(self.CREATE_RESPONSE)
    self.mox.ReplayAll()

    self.stage_cli.dispatch(args, self.OPTIONS)

  def test_dispatch_reset(self):
    version_id = '14'
    args = ['reset', self.JOB_KEY, version_id]

    self.stage_api.reset(self.AURORA_JOB_KEY, int(version_id), self.PROXY_HOST)
    self.mox.ReplayAll()

    self.stage_cli.dispatch(args, self.OPTIONS)

  def test_log_long(self):
    log = """Version: 1 (md5: 5be07da9642fb3ec5bc0df0c1290dada) (Currently released)
Created by: johndoe
Date created: %s
Released by: jane
Date released: %s
    testing
    messages
""" % (str(datetime.fromtimestamp(self.STAGED_CONFIG_CREATE_TIMESTAMP / 1000)),
       str(datetime.fromtimestamp(self.STAGED_CONFIG_RELEASE_TIMESTAMP / 1000)))

    assert StagedConfigFormat.long_str(self.STAGED_CONFIG) == log

  def test_log_one_line(self):
    log = "1 - testing (%s) <johndoe> (RELEASED)" % str(
        datetime.fromtimestamp(self.STAGED_CONFIG_CREATE_TIMESTAMP / 1000))
    assert StagedConfigFormat.one_line_str(self.STAGED_CONFIG) == log
