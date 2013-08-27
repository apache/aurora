from twitter.aurora.common.cluster import Cluster

from pystachio import (
    Default,
    Integer,
    Required,
    String)
import pytest


def test_simple():
  class AudubonTrait(Cluster.Trait):
    master_role = String
    slave_role  = Default(String, 'slave')
    version     = Required(Integer)

  west = Cluster(name = 'west',
                 master_role = 'west.master',
                 slave_role = 'west.slave',
                 version = 10)
  east = Cluster(name = 'east', version = 11)

  assert east.name == 'east'
  with pytest.raises(AttributeError):
    east.slave_role
  assert east.with_traits(AudubonTrait).slave_role == 'slave'
  assert west.with_traits(AudubonTrait).slave_role == 'west.slave'
  assert east.with_traits(AudubonTrait).master_role is None

  with pytest.raises(TypeError):
    # requires version at least
    Cluster(name = 'east').with_traits(AudubonTrait)
