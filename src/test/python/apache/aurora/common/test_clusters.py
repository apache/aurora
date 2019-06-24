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

import json
import os

import pytest
from twitter.common.contextutil import temporary_dir

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters

CLUSTERS = '''[
  {
    "name": "cluster1",
    "zk": "zookeeper.cluster1.example.com",
    "slave_run_directory": "latest",
    "slave_root": "/var/lib/mesos",
    "auth_mechanism": "UNAUTHENTICATED"
  },
  {
    "name": "cluster2",
    "zk": "zookeeper.example.com",
    "slave_run_directory": "latest",
    "slave_root": "/var/lib/mesos",
    "auth_mechanism": "UNAUTHENTICATED"
  }
]
'''


def validate_loaded_clusters(clusters):
  assert '__default' not in clusters
  for cluster_name in ('cluster1', 'cluster2'):
    assert cluster_name in clusters
    cluster = clusters[cluster_name]
    assert cluster.name == cluster_name
    assert cluster.slave_root == '/var/lib/mesos'
    assert cluster.slave_run_directory == 'latest'
    assert cluster.auth_mechanism == 'UNAUTHENTICATED'
  assert clusters['cluster1'].zk == 'zookeeper.cluster1.example.com'


def test_load():
  with temporary_dir() as td:
    clusters_json = os.path.join(td, 'clusters.json')
    with open(clusters_json, 'w') as fp:
      fp.write(CLUSTERS)
    validate_loaded_clusters(Clusters.from_file(clusters_json))


def test_load_invalid_syntax():
  with temporary_dir() as td:
    # bad json
    clusters_json = os.path.join(td, 'clusters.json')
    with open(clusters_json, 'w') as fp:
      fp.write('This is not json')
    with pytest.raises(Clusters.ParseError):
      Clusters.from_file(clusters_json)

    # not a dict
    clusters_json = os.path.join(td, 'clusters.json')
    with open(clusters_json, 'w') as fp:
      fp.write(json.dumps({'cluster1': ['not', 'cluster', 'values']}))
    with pytest.raises(Clusters.ParseError):
      Clusters.from_file(clusters_json)


def test_patch_cleanup_on_error():
  clusters = Clusters([Cluster(name='original')])

  with pytest.raises(RuntimeError):
    with clusters.patch([Cluster(name='replacement')]):
      assert list(clusters) == ['replacement']
      raise RuntimeError("exit contextmanager scope")

  assert list(clusters) == ['original']
