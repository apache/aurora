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

from optparse import OptionParser

import pytest

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.cluster_option import ClusterOption
from apache.aurora.common.clusters import Clusters

CLUSTER_LIST = Clusters((
  Cluster(name='smf1'),
  Cluster(name='smf1-test'),
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


class MockOptionParser(OptionParser):
  class Error(Exception): pass

  def error(self, msg):
    # per optparse documentation:
    # Print a usage message incorporating 'msg' to stderr and exit.
    # If you override this in a subclass, it should not return -- it
    # should either exit or raise an exception.
    raise self.Error(msg)


def make_parser():
  parser = MockOptionParser()
  parser.add_option(ClusterOption('--source_cluster', '-s', clusters=CLUSTER_LIST))
  parser.add_option(ClusterOption('--dest_cluster', clusters=CLUSTER_LIST))
  parser.add_option(ClusterOption('--cluster', cluster_provider=cluster_provider))
  return parser


def test_parsable():
  parser = make_parser()
  values, _ = parser.parse_args(['--source_cluster=smf1-test', '--cluster=smf1-test'])
  assert isinstance(values.source_cluster, Cluster)
  assert isinstance(values.cluster, Cluster)


def test_not_parsable():
  parser = make_parser()
  try:
    parser.parse_args(['--source_cluster=borg'])
  except MockOptionParser.Error as e:
    assert 'borg is not a valid cluster for the --source_cluster option.' in e.args[0]
  else:
    assert False, 'Expected OptionParser to raise on invalid cluster list.'
