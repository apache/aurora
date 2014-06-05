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

import functools

import mox
import pytest

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.api.disambiguator import LiveJobDisambiguator
from apache.aurora.common.aurora_job_key import AuroraJobKey
from apache.aurora.common.cluster import Cluster

from gen.apache.aurora.api.constants import ResponseCode
from gen.apache.aurora.api.ttypes import GetJobsResult, JobConfiguration, JobKey, Response, Result

TEST_CLUSTER = Cluster(name='smf1')


class LiveJobDisambiguatorTest(mox.MoxTestBase):
  CLUSTER = TEST_CLUSTER
  ROLE = 'mesos'
  ENV = 'test'
  NAME = 'labrat'
  JOB_PATH = 'smf1/mesos/test/labrat'
  CONFIG_FILE = 'abc.aurora'

  def setUp(self):
    super(LiveJobDisambiguatorTest, self).setUp()
    self._api = self.mox.CreateMock(AuroraClientAPI)
    self._api.cluster = self.CLUSTER
    self._options = self.mox.CreateMockAnything()
    self._options.cluster = self.CLUSTER

  def test_ambiguous_property(self):
    assert LiveJobDisambiguator(self._api, self.ROLE, None, self.NAME).ambiguous
    assert not LiveJobDisambiguator(self._api, self.ROLE, self.ENV, self.NAME).ambiguous

  def _expect_get_jobs(self, *envs):
    self._api.get_jobs(self.ROLE).AndReturn(Response(
      responseCode=ResponseCode.OK,
      messageDEPRECATED='Mock OK',
      result=Result(getJobsResult=GetJobsResult(
        configs=set(JobConfiguration(key=JobKey(role=self.ROLE, environment=env, name=self.NAME))
        for env in envs)))))

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
    cluster_name, role, env, name = key
    assert cluster_name == self.CLUSTER.name
    assert role == self.ROLE
    assert env == self.ENV
    assert name == self.NAME

  def test_disambiguate_args_or_die_unambiguous_with_no_config(self):
    expected = (self._api, AuroraJobKey(self.CLUSTER.name, self.ROLE, self.ENV, self.NAME), None)
    result = LiveJobDisambiguator.disambiguate_args_or_die([self.JOB_PATH], None,
        client_factory=lambda *_: self._api)
    assert result == expected

  def test_disambiguate_args_or_die_unambiguous_with_config(self):
    expected = (self._api,
        AuroraJobKey(self.CLUSTER.name, self.ROLE, self.ENV, self.NAME), self.CONFIG_FILE)
    result = LiveJobDisambiguator.disambiguate_args_or_die([self.JOB_PATH, self.CONFIG_FILE], None,
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
