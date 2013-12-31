import pytest

from twitter.common import options

from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import Clusters
from apache.aurora.common.cluster_option import ClusterOption


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
