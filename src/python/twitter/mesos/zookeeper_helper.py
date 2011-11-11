import sys
import time
import zookeeper

import clusters
from location import Location
from tunnel_helper import TunnelHelper

from twitter.common import log

class ZookeeperHelper(object):
  ZOOKEEPER_PORT = 2181
  LOCAL_ZK_TUNNEL_PORT = 9999

  @staticmethod
  def create_zookeeper_tunnel(cluster):
    host, port = TunnelHelper.create_tunnel(
      TunnelHelper.get_tunnel_host(cluster),
      ZookeeperHelper.LOCAL_ZK_TUNNEL_PORT,
      clusters.get_zk_host(cluster),
      ZookeeperHelper.ZOOKEEPER_PORT)
    return host, port

  @staticmethod
  def get_zookeeper_handle(cluster, port=2181):
    """ Get a zookeeper connection reachable from this machine.
    by location. Sets up ssh tunnels as appropriate.
    """
    host = clusters.get_zk_host(cluster)
    port = ZookeeperHelper.ZOOKEEPER_PORT

    if host is not 'localhost' and Location.is_corp():
      host, port = ZookeeperHelper.create_zookeeper_tunnel(cluster)
    log.info('Initialzing zookeeper client')
    return zookeeper.init('%s:%d' % (host, port))

  @staticmethod
  def get_zookeeper_children_or_die(zh, path):
    """Read children from a specific path on a given zookeeper.

    The first read often fails, due (I believe) to the appropriate ssh tunnels
    not being fully live yet. We make several attemps, and sleep in between each
    one. We fail after several attemps.
    """
    NUM_ATTEMPTS = 10
    for i in range(NUM_ATTEMPTS):
      log.debug('Reading children from zookeeper path %s (Attempt %d/%d).' % (
          path, i + 1, NUM_ATTEMPTS))
      try:
        children = zookeeper.get_children(zh, path)
        if children:
          return children
        else:
          log.debug("Empty children list: retrying...")
      except Exception, e:
        log.debug("Can't get children from zookeper [%s]. Retrying..." % e)
      time.sleep(1)
    log.fatal("Can't talk to zookeeper.")
    sys.exit(1)
