import random
import sys

from twitter.common import log
from twitter.common.net.tunnel import TunnelHelper
from twitter.common.zookeeper.serverset import ServerSet
from twitter.common_internal.location import Location
from twitter.common_internal.zookeeper.tunneler import TunneledZookeeper

from twitter.mesos.clusters import Cluster

from .packer_client import Packer


def create_packer(cluster, verbose=False):
  cluster = Cluster.get(cluster)
  zk = TunneledZookeeper.get(cluster.packer_zk, verbose=verbose)
  packer_ss = ServerSet(zk, cluster.packer_zk_path)
  packers = list(packer_ss)
  zk.close()

  if len(packers) == 0:
    log.fatal('Could not find any packers!')
    sys.exit(1)

  log.debug('Found nodes:')
  for packer in packers:
    log.debug('  %s' % packer)
  random.shuffle(packers)

  packer = packers[0]
  packer_host, packer_port = packer.service_endpoint.host, packer.service_endpoint.port
  log.debug('Selecting host %s:%s' % (packer_host, packer_port))

  if Location.is_corp():
    packer_host, packer_port = TunnelHelper.create_tunnel(packer_host, packer_port)

  return Packer(packer_host, packer_port, verbose=verbose)
