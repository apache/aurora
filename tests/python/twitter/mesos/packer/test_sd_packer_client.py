import unittest

from twitter.mesos.common.clusters import Clusters
from twitter.mesos.common_internal.clusters import TwitterCluster
from twitter.mesos.packer.packer_client import Packer

import twitter.mesos.packer.sd_packer_client as sd_packer_client

class TestSdPackerClient(unittest.TestCase):
  def test_packer_uri_override(self):
    clusters = Clusters((
      TwitterCluster(
        name='test_cluster',
        packer_uri='localhost:8000'
      )
    ))
    packer = sd_packer_client.create_packer('test_cluster', False, clusters)
    assert packer._host == "localhost"
    assert packer._port == 8000

  def test_packer_uri_override_with_bad_port(self):
    clusters = Clusters((
      TwitterCluster(
        name='test_cluster',
        packer_uri='localhost:8000asd'
      )
    ))
    self.assertRaises(Packer.Error, sd_packer_client.create_packer, 'test_cluster', False, clusters)

  def test_packer_uri_override_with_missing_parts(self):
    clusters = Clusters((
      TwitterCluster(
        name='test_cluster',
        packer_uri='localhost-no-port'
      )
    ))
    self.assertRaises(Packer.Error, sd_packer_client.create_packer, 'test_cluster', False, clusters)
