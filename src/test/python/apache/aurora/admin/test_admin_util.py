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

import subprocess
import unittest

import mock
from twitter.common.contextutil import temporary_file

from apache.aurora.admin.admin_util import make_admin_client, parse_script
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters


class TestAdminUtil(unittest.TestCase):
  TEST_CLUSTER_NAME = 'west'
  TEST_CLUSTER = Cluster(
          name=TEST_CLUSTER_NAME,
          zk='zookeeper.example.com',
          scheduler_zk_path='/foo/bar',
          auth_mechanism='UNAUTHENTICATED')
  TEST_CLUSTERS = Clusters([TEST_CLUSTER])

  @mock.patch("apache.aurora.admin.admin_util.subprocess", spec=subprocess)
  def test_parse_script(self, mock_subprocess):
    with temporary_file() as fp:
      mock_popen = mock.Mock()
      mock_popen.wait.return_value = 0
      mock_subprocess.Popen.return_value = mock_popen
      parse_script(fp.name)('h1')
      assert mock_popen.wait.call_count == 1

  def test_parse_script_invalid_filename(self):
    self.assertRaises(SystemExit, parse_script, "invalid filename")

  def test_make_admin_client_cluster_string(self):
    with mock.patch('apache.aurora.admin.admin_util.CLUSTERS', new=self.TEST_CLUSTERS):
      self.assertIsNotNone(make_admin_client(self.TEST_CLUSTER_NAME))

  def test_make_admin_client_cluster_object(self):
    with mock.patch('apache.aurora.admin.admin_util.CLUSTERS', new=self.TEST_CLUSTERS):
      self.assertIsNotNone(make_admin_client(self.TEST_CLUSTER))

  def test_make_admin_client_cluster_unknown(self):
    with mock.patch('apache.aurora.admin.admin_util.CLUSTERS', new=self.TEST_CLUSTERS):
      self.assertRaises(SystemExit, make_admin_client, 'east')
