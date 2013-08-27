from collections import Mapping
import json

from twitter.common.collections import maybe_list

from .cluster import Cluster

from pystachio import Required, String


__all__ = ('Clusters',)


class NameTrait(Cluster.Trait):
  name = Required(String)


class Clusters(Mapping):
  class Error(Exception): pass
  class ClusterExists(Error): pass

  @classmethod
  def from_yml(cls, filename):
    raise NotImplementedError

  @classmethod
  def from_json(cls, filename):
    with open(filename) as fp:
      return cls([Cluster(**cluster_dict) for cluster_dict in json.load(fp)])

  def __init__(self, cluster_list):
    self.replace(cluster_list)

  def replace(self, cluster_list):
    self._clusters = {}
    cluster_list = maybe_list(cluster_list, expected_type=Cluster, raise_type=TypeError)
    for cluster in cluster_list:
      self.add(cluster)

  def add(self, cluster):
    """Add a cluster to this Clusters map."""
    cluster = Cluster(**cluster)
    cluster.check_trait(NameTrait)
    if cluster.name in self:
      raise self.ClusterExists('Cluster %s already exists' % cluster.name)
    self._clusters[cluster.name] = cluster

  def __iter__(self):
    return iter(self._clusters)

  def __len__(self):
    return len(self._clusters)

  def __getitem__(self, name):
    return self._clusters[name]
