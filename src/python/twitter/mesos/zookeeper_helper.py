import os
import zookeeper

from twitter.common.net.tunnel import TunnelHelper
from twitter.common.zookeeper.client import ZooKeeper
from twitter.common.zookeeper.serverset import ServerSet
from twitter.mesos.clusters import Cluster
from twitter.mesos.location import Location

from twitter.common import log

class ZookeeperHelper(object):
  ZOOKEEPER_SILENCED = False

  @classmethod
  def quiet_zookeeper(cls):
    if not cls.ZOOKEEPER_SILENCED:
      zookeeper.set_log_stream(open(os.devnull, 'w'))
      cls.ZOOKEEPER_SILENCED = True

  @classmethod
  def get_zookeeper_handle(cls, host, port=2181):
    """ Get a zookeeper connection reachable from this machine.
    by location. Sets up ssh tunnels as appropriate.
    """
    if host is not 'localhost' and Location.is_corp():
      host, port = TunnelHelper.create_tunnel(host, port)
    log.info('Initializing zookeeper client on %s:%d' % (host, port))
    cls.quiet_zookeeper()
    return ZooKeeper('%s:%d' % (host, port))

  @classmethod
  def get_scheduler_serverset(cls, cluster, port=2181, **kw):
    zk = cls.get_zookeeper_handle(Cluster.get(cluster).zk, port=port)
    return zk, ServerSet(zk, Cluster.get(cluster).scheduler_zk_path, **kw)

  @classmethod
  def get_packer_serverset(cls, cluster, port=2181, **kw):
    zk = cls.get_zookeeper_handle(Cluster.get(cluster).packer_zk, port=port)
    return zk, ServerSet(zk, Cluster.get(cluster).packer_zk_path, **kw)
