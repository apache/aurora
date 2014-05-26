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

import pytest
from twitter.common import options

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.cluster_option import ClusterOption
from apache.aurora.common.clusters import Clusters

CLUSTER_LIST = Clusters((
  Cluster(name = 'smf1'),
  Cluster(name = 'smf1-test'),
))


def cluster_provider(name):
  return CLUSTER_LIST[name]


def test_constructors():
  ClusterOption('--test', '-t', help="Test cluster.", clusters=CLUSTER_LIST)
  ClusterOption('--test', '-t', help="Test cluster.", cluster_provider=cluster_provider)

  with pytest.raises(ValueError):
    ClusterOption()

  with pytest.raises(ValueError):
    ClusterOption('--cluster')  # requires clusters=

  with pytest.raises(ValueError):
    ClusterOption('--cluster', clusters=CLUSTER_LIST, cluster_provider=cluster_provider)


def test_parsable(capsys):
  parser = options.parser().options((
    ClusterOption('--source_cluster', '-s', clusters=CLUSTER_LIST),
    ClusterOption('--dest_cluster', clusters=CLUSTER_LIST),
    ClusterOption('--cluster', cluster_provider=cluster_provider)))

  values, _ = parser.parse(['--source_cluster=smf1-test', '--cluster=smf1-test'])
  assert isinstance(values.source_cluster, Cluster)
  assert isinstance(values.cluster, Cluster)

  with pytest.raises(SystemExit):
    parser.parse(['--source_cluster=borg'])

  out, err = capsys.readouterr()
  assert 'error: borg is not a valid cluster for the --source_cluster option.' in err
