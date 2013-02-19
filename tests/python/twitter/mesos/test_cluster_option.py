import exceptions
import pytest

from twitter.common import options
from twitter.mesos.clusters import Cluster
from twitter.mesos.cluster_option import ClusterOption

def test_constructors():
  ClusterOption('--cluster')
  ClusterOption('--test', '-t', default='smf1-test', help="Test cluster.")

  with pytest.raises(exceptions.TypeError):
    ClusterOption()

  with pytest.raises(Cluster.UnknownCluster):
    ClusterOption('--cluster', default='borg')

def test_parsable(capsys):
  parser = options.parser().options((
    ClusterOption('--source_cluster', '-s'),
    ClusterOption('--dest_cluster', default='smf1-test'),
    ClusterOption('--cluster',)))

  values, _ = parser.parse(['--source_cluster=smf1-test', '--cluster=smf1-test'])
  assert isinstance(values.source_cluster, Cluster)
  assert isinstance(values.dest_cluster, Cluster)
  assert isinstance(values.cluster, Cluster)

  with pytest.raises(SystemExit):
    parser.parse(['--source_cluster=borg'])
  out, err = capsys.readouterr()
  assert 'error: borg is not a valid cluster for --source_cluster option.' in err

def test_filter():
  option = ClusterOption('--cluster', cluster_filter=lambda cluster: cluster.name != 'smf1-test')
  parser = options.parser().options([option])
  parser.parse(['--cluster=smf1'])
  with pytest.raises(SystemExit):
    parser.parse(['--cluster=smf1-test'])

def test_usage_output(capsys):
  option = ClusterOption('--cluster', cluster_filter=lambda cluster: cluster.name != 'smf1',
    default='smf1-test')
  parser = options.parser().options([option])
  with pytest.raises(SystemExit):
    parser.parse(['--cluster=smf1'])
  out, err = capsys.readouterr()
  assert 'error: smf1 is not a valid cluster for --cluster option.' in err
