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

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters

TEST_CLUSTER = 'west'


TEST_CLUSTERS = Clusters([Cluster(
  name=TEST_CLUSTER,
  zk='zookeeper.example.com',
  scheduler_zk_path='/foo/bar',
  auth_mechanism='UNAUTHENTICATED')])
