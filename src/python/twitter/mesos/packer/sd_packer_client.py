import os
import random
import sys

from twitter.common import log
from twitter.common.net.tunnel import TunnelHelper
from twitter.mesos.location import Location
from twitter.mesos.zookeeper_helper import ZookeeperHelper

from twitter.mesos.packer.packer_client import Packer

_ZK_TIMEOUT_SECS = 5


def create_packer(cluster):
  zk, packer_ss = ZookeeperHelper.get_packer_serverset(cluster)
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

  return Packer(packer_host, packer_port)
