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

import unittest

import pytest
from mock import call, patch
from twitter.common.contextutil import temporary_file

from apache.aurora.client import binding_helper
from apache.aurora.client.binding_helper import BindingHelper
from apache.aurora.client.binding_helpers.docker_helper import DockerBindingHelper
from apache.aurora.client.docker.docker_client import DockerRegistryClient
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import CLUSTERS
from apache.aurora.config import AuroraConfig

DOCKER_BINDING_CONFIG = """
jobs = [Job(
  name = 'hello_world',
  role = 'john_doe',
  environment = 'staging42',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name='command', cmdline="echo helloworld")],
    resources = Resources(cpu = 0.1, ram = 64 * MB, disk = 64 * MB),
  ),
  container = Docker(image='{{docker.image[some/name][some.tag]}}')
)]
"""

TEST_CLUSTER = Cluster(
    name='smf1-test',
    docker_registry='some.registry.domain.com')
TEST_CLUSTERS = [TEST_CLUSTER]


class TestDockerBindingHelper(unittest.TestCase):

  @patch.object(DockerRegistryClient, 'resolve')
  def test_docker_binding(self, mock_resolve):
    image_reference = 'registry.example.com/some/repo@some:digest'

    mock_resolve.return_value = image_reference

    binding_helper.unregister_all()
    BindingHelper.register(DockerBindingHelper())

    with temporary_file() as fp:
      fp.write(DOCKER_BINDING_CONFIG)
      fp.flush()
      with CLUSTERS.patch(TEST_CLUSTERS):
        cfg = AuroraConfig.load(fp.name)
        binding_helper.apply_all(cfg)
        assert cfg.job().taskConfig.container.docker.image == image_reference
        assert mock_resolve.mock_calls == [call(TEST_CLUSTER, 'some/name', 'some.tag')]

  @patch.object(DockerRegistryClient, 'resolve')
  def test_docker_binding_throws(self, mock_resolve):
    mock_resolve.side_effect = Exception('mock resolve failure')

    binding_helper.unregister_all()
    BindingHelper.register(DockerBindingHelper())

    with temporary_file() as fp:
      fp.write(DOCKER_BINDING_CONFIG)
      fp.flush()
      with CLUSTERS.patch(TEST_CLUSTERS):
        cfg = AuroraConfig.load(fp.name)
        with pytest.raises(Exception):
          binding_helper.apply_all(cfg)
          assert mock_resolve.mock_calls == [call(TEST_CLUSTER, 'some/name', 'some.tag')]
