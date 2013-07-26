import random
import sys

from twitter.common import log
from twitter.common.net.tunnel import TunnelHelper
from twitter.common.zookeeper.serverset import ServerSet
from twitter.common_internal.location import Location
from twitter.common_internal.zookeeper.tunneler import TunneledZookeeper

from twitter.mesos.common.cluster import Cluster
# TODO(wickman) Push this into mesos_client w/ MESOS-3006
from twitter.mesos.common_internal.clusters import TWITTER_CLUSTERS

from .packer_client import Packer

from pystachio import Required, String


class PackerTrait(Cluster.Trait):
  packer_zk = String
  packer_zk_path = String
  packer_redirect = String
  packer_uri = String


def create_packer(cluster_name, verbose=False, clusters=TWITTER_CLUSTERS):
  while True:
    cluster = clusters[cluster_name].with_trait(PackerTrait)
    if not cluster.packer_redirect:
      break
    log.info('Redirecting %s to packer cluster in %s' % (cluster.name, cluster.packer_redirect))
    cluster_name = cluster.packer_redirect

  if cluster.packer_uri:
    try:
      packer_host, port = cluster.packer_uri.split(':')
      packer_port = int(port)
    except ValueError:
      raise Packer.Error('Invalid packer_uri: %s' % cluster.packer_uri)

    log.debug('Using packer host from cluster config %s:%d' % (packer_host, packer_port))
  elif not cluster.packer_zk or not cluster.packer_zk_path:
    log.fatal('%s does not have a packer defined!' % cluster.name)
    # TODO(wickman) This should raise, not sys exit.
    sys.exit(1)
  else:
    zk = TunneledZookeeper.get(cluster.packer_zk, verbose=verbose)
    packer_ss = ServerSet(zk, cluster.packer_zk_path)
    packers = list(packer_ss)
    zk.close()

    if len(packers) == 0:
      log.fatal('Could not find any packers!')
      # TODO(wickman) This should raise, not sys exit.
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
