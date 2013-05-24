import functools

from twitter.mesos.client.api import MesosClientAPI
from twitter.mesos.client.disambiguator import LiveJobDisambiguator
from twitter.mesos.common import AuroraJobKey

from gen.twitter.mesos.constants import ResponseCode
from gen.twitter.mesos.ttypes import (
   GetJobsResponse,
   JobConfiguration,
   JobKey,
)

import mox
import pytest


class LiveJobDisambiguatorTest(mox.MoxTestBase):
  CLUSTER = 'smf1'
  ROLE = 'mesos'
  ENV = 'test'
  NAME = 'labrat'
  JOB_PATH = 'smf1/mesos/test/labrat'

  def setUp(self):
    super(LiveJobDisambiguatorTest, self).setUp()
    self._api = self.mox.CreateMock(MesosClientAPI)
    self._api.cluster = self.CLUSTER
    self._options = self.mox.CreateMockAnything()
    self._options.cluster = self.CLUSTER

  def test_ambiguous_property(self):
    assert LiveJobDisambiguator(self._api, self.ROLE, None, self.NAME).ambiguous
    assert not LiveJobDisambiguator(self._api, self.ROLE, self.ENV, self.NAME).ambiguous

  def _expect_get_jobs(self, *envs):
    self._api.get_jobs(self.ROLE).AndReturn(GetJobsResponse(
      responseCode=ResponseCode.OK,
      message='Mock OK',
      configs=set(JobConfiguration(key=JobKey(role=self.ROLE, environment=env, name=self.NAME))
        for env in envs)))

  def _try_disambiguate_ambiguous(self):
    return LiveJobDisambiguator._disambiguate_or_die(self._api, self.ROLE, None, self.NAME)

  def test_disambiguate_or_die_ambiguous(self):
    self._expect_get_jobs('test')
    self._expect_get_jobs('prod')
    self._expect_get_jobs('devel', 'test')
    self._expect_get_jobs()

    self.mox.ReplayAll()

    _, _, env1, _ = self._try_disambiguate_ambiguous()
    assert env1 == 'test'

    _, _, env2, _ = self._try_disambiguate_ambiguous()
    assert env2 == 'prod'

    with pytest.raises(SystemExit):
      self._try_disambiguate_ambiguous()

    with pytest.raises(SystemExit):
      self._try_disambiguate_ambiguous()

  def test_disambiguate_job_path_or_die_unambiguous(self):
    key = LiveJobDisambiguator._disambiguate_or_die(self._api, self.ROLE, self.ENV, self.NAME)
    cluster, role, env, name = key
    assert cluster == self.CLUSTER
    assert role == self.ROLE
    assert env == self.ENV
    assert name == self.NAME

  def test_disambiguate_args_or_die_unambiguous(self):
    expected = (self._api, AuroraJobKey(self.CLUSTER, self.ROLE, self.ENV, self.NAME))
    result = LiveJobDisambiguator.disambiguate_args_or_die([self.JOB_PATH], None,
        client_factory=lambda *_: self._api)
    assert result == expected

  def test_disambiguate_args_or_die_ambiguous(self):
    self._expect_get_jobs('test')
    self._expect_get_jobs('prod', 'devel')
    self._expect_get_jobs()

    disambiguate_args_or_die = functools.partial(LiveJobDisambiguator.disambiguate_args_or_die,
        (self.ROLE, self.NAME), self._options, lambda *_: self._api)

    self.mox.ReplayAll()

    disambiguate_args_or_die()

    with pytest.raises(SystemExit):
      disambiguate_args_or_die()

    with pytest.raises(SystemExit):
      disambiguate_args_or_die()
