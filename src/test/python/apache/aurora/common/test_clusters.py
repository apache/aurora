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
import yaml
from twitter.common.contextutil import temporary_dir

from apache.aurora.common.clusters import Clusters, Parser

TEST_YAML = """
__default: &default
  force_notunnel: no
  slave_root: /var/lib/mesos
  slave_run_directory: latest
  zk: zookeeper.example.com
  auth_mechanism: UNAUTHENTICATED

cluster1:
  <<: *default
  name: cluster1
  dc: cluster1
  zk: zookeeper.cluster1.example.com

cluster2:
  <<: *default
  name: cluster2
  dc: cluster2
"""

CLUSTERS = yaml.load(TEST_YAML)


def validate_loaded_clusters(clusters):
  assert '__default' not in clusters
  for cluster_name in ('cluster1', 'cluster2'):
    assert cluster_name in clusters
    cluster = clusters[cluster_name]
    assert cluster.name == cluster_name
    assert cluster.dc == cluster_name
    assert cluster.force_notunnel is False
    assert cluster.slave_root == '/var/lib/mesos'
    assert cluster.slave_run_directory == 'latest'
    assert cluster.auth_mechanism == 'UNAUTHENTICATED'
  assert clusters['cluster1'].zk == 'zookeeper.cluster1.example.com'


def test_load_json():
  with temporary_dir() as td:
    clusters_json = os.path.join(td, 'clusters.json')
    # as dict
    with open(clusters_json, 'w') as fp:
      fp.write(json.dumps(CLUSTERS))
    validate_loaded_clusters(Clusters.from_file(clusters_json))
    # as list
    with open(clusters_json, 'w') as fp:
      fp.write(json.dumps(CLUSTERS.values()))
    validate_loaded_clusters(Clusters.from_file(clusters_json))


def test_load_yaml():
  with temporary_dir() as td:
    clusters_yml = os.path.join(td, 'clusters.yml')
    with open(clusters_yml, 'w') as fp:
      fp.write(TEST_YAML)
    validate_loaded_clusters(Clusters.from_file(clusters_yml))


def test_load_without_yaml_loader():
  class NoYamlClusters(Clusters):
    LOADERS = {'.json': Parser(json.load, ValueError)}
  with temporary_dir() as td:
    clusters_yml = os.path.join(td, 'clusters.yml')
    with open(clusters_yml, 'w') as fp:
      fp.write(TEST_YAML)
    with pytest.raises(Clusters.UnknownFormatError):
      NoYamlClusters.from_file(clusters_yml)


def test_load_invalid_syntax():
  with temporary_dir() as td:
    # bad json
    clusters_json = os.path.join(td, 'clusters.json')
    with open(clusters_json, 'w') as fp:
      fp.write('This is not json')
    with pytest.raises(Clusters.ParseError):
      Clusters.from_file(clusters_json)

    # bad yaml
    clusters_yml = os.path.join(td, 'clusters.yml')
    with open(clusters_yml, 'w') as fp:
      fp.write('L{}L')
    with pytest.raises(Clusters.ParseError):
      Clusters.from_file(clusters_yml)

    # bad layout
    clusters_yml = os.path.join(td, 'clusters.yml')
    with open(clusters_yml, 'w') as fp:
      fp.write('just a string')
    with pytest.raises(Clusters.ParseError):
      Clusters.from_file(clusters_yml)

    # not a dict
    clusters_json = os.path.join(td, 'clusters.json')
    with open(clusters_json, 'w') as fp:
      fp.write(json.dumps({'cluster1': ['not', 'cluster', 'values']}))
    with pytest.raises(Clusters.ParseError):
      Clusters.from_file(clusters_json)
