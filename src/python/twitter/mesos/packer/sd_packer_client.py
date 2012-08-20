import os
import random

from twitter.common import log
from twitter.common.net.tunnel import TunnelHelper
from twitter.common.zookeeper.client import ZooKeeper
from twitter.common.zookeeper.group import Group
from twitter.mesos.location import Location
from twitter.mesos.util import serverset_util

from twitter.mesos.packer.packer_client import Packer

_ZK_TIMEOUT_SECS = 5

def create_packer(cluster):
  zk_host, zk_port = cluster.packer_zk, 2181
  if Location.is_corp():
    zk_host, zk_port = TunnelHelper.create_tunnel(zk_host, zk_port)

  zk_client = ZooKeeper(servers='%s:%d' % (zk_host, zk_port), timeout_secs=_ZK_TIMEOUT_SECS)

  nodes = zk_client.get_children(cluster.packer_zk_path)
  log.debug('Found nodes: %s' % ', '.join(nodes))
  random.shuffle(nodes)
  target_node = nodes[0]
  data, _ = zk_client.get(os.path.join(cluster.packer_zk_path, target_node))

  instance = serverset_util.decode_service_instance(data)
  packer_host, packer_port = instance.serviceEndpoint.host, instance.serviceEndpoint.port

  log.debug('Selecting host %s:%s' % (packer_host, packer_port))
  if Location.is_corp():
    packer_host, packer_port = TunnelHelper.create_tunnel(packer_host, packer_port)
  # TODO(wfarner): Fix this in TunnelHelper. We sleep since the tunnel process may still be
  #   starting but not established.
  import time
  time.sleep(1)
  return Packer(packer_host, packer_port)