"""Get a handle to the scheduler, working around prod/corp networking issues.
"""

import os
import sys
import time

from twitter.common import log
from twitter.common.net.tunnel import TunnelHelper
from twitter.mesos.clusters import Cluster
from twitter.mesos.location import Location
from twitter.mesos.zookeeper_helper import ZookeeperHelper

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket, TSSLSocket, TTransport

from gen.twitter.mesos import MesosAdmin


class SchedulerClient(object):
  THRIFT_RETRIES = 5
  RETRY_TIMEOUT_SECS = 1.0

  class CouldNotConnect(Exception): pass

  @staticmethod
  def get(cluster, **kwargs):
    log.debug('Client location: %s' % ('prod' if Location.is_prod() else 'corp'))

    # TODO(vinod) : Clear up the convention of what --cluster <arg> means
    # Currently arg can be any one of
    # 'localhost:<scheduler port>'
    # '<cluster name>'
    # '<cluster name>: <zk port>'
    # Instead of encoding zk port inside the <arg> string make it explicit.
    if cluster.startswith('localhost:'):
      log.info('Attempting to talk to local scheduler.')
      port = int(cluster.split(':')[1])
      return LocalSchedulerClient(port, ssl=True)
    else:
      zk_port = 2181 if ':' not in cluster else int(cluster.split(':')[1])
      return ZookeeperSchedulerClient(cluster, zk_port, ssl=True, **kwargs)

  def __init__(self, verbose=False, ssl=False):
    self._client = None
    self._verbose = verbose
    self._ssl = ssl

  def get_thrift_client(self):
    if self._client is None:
      self._client = self._connect()
    return self._client

  # per-class implementation -- mostly meant to set up a valid host/port
  # pair and then delegate the opening to SchedulerClient._connect_scheduler
  def _connect(self):
    return None

  @staticmethod
  def _connect_scheduler(host, port, with_ssl=False):
    if with_ssl:
      socket = TSSLSocket.TSSLSocket(host, port, validate=False)
    else:
      socket = TSocket.TSocket(host, port)
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    schedulerClient = MesosAdmin.Client(protocol)
    for _ in range(SchedulerClient.THRIFT_RETRIES):
      try:
        transport.open()
        return schedulerClient
      except TTransport.TTransportException:
        time.sleep(SchedulerClient.RETRY_TIMEOUT_SECS)
        continue
    raise SchedulerClient.CouldNotConnect('Could not connect to %s:%s' % (host, port))


class ZookeeperSchedulerClient(SchedulerClient):
  SCHEDULER_ZK_PATH = '/twitter/service/mesos-scheduler'

  def __init__(self, cluster, port=2181, ssl=False, verbose=False):
    SchedulerClient.__init__(self, verbose=verbose, ssl=ssl)
    self._cluster = cluster
    self._zkport = port
    self._endpoint = None

  def _connect(self):
    serverset = ZookeeperHelper.get_scheduler_serverset(self._cluster, port=self._zkport)
    serverset_endpoints = list(serverset)
    if len(serverset_endpoints) == 0:
      raise self.CouldNotConnect('No schedulers detected in %s!' % self._cluster)
    instance = serverset_endpoints[0]
    self._endpoint = instance.service_endpoint
    self._http = instance.additional_endpoints.get('http')
    host, port = self._maybe_tunnel(self._cluster, self._endpoint.host, self._endpoint.port)
    return SchedulerClient._connect_scheduler(host, port, self._ssl)

  @classmethod
  def _maybe_tunnel(cls, cluster, host, port):
    # Open a tunnel to the scheduler if necessary
    if Location.is_corp() and not Cluster.get(cluster).force_notunnel:
      log.info('Creating ssh tunnel for %s' % cluster)
      return TunnelHelper.create_tunnel(host, port)
    return host, port

  @property
  def url(self):
    proxy_url = Cluster.get(self._cluster).proxy_url
    if proxy_url:
      return proxy_url
    if self._http:
      return 'http://%s:%s' % (self._http.host, self._http.port)


class LocalSchedulerClient(SchedulerClient):
  def __init__(self, port, ssl=False):
    SchedulerClient.__init__(self, verbose=True, ssl=ssl)
    self._host = 'localhost'
    self._port = port

  def _connect(self):
    return SchedulerClient._connect_scheduler(self._host, self._port, with_ssl=self._ssl)

  @property
  def url(self):
    return 'http://localhost:8081'
