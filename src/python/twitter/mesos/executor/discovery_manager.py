import socket
import threading

from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset import Endpoint
from twitter.common_internal.zookeeper.twitter_service import TwitterService

from .health_interface import HealthInterface


class DiscoveryManager(HealthInterface):
  DEFAULT_ACL_PATH = '/etc/twkeys/mesos/zookeeper/service.yml'

  @staticmethod
  def join_keywords(hostname, portmap, primary_port):
    """
      Generate primary, additional endpoints from a portmap and primary_port.
      primary_port must be a name in the portmap dictionary.
    """
    # Do int check as stop-gap measure against incompatible downstream clients.
    additional_endpoints = dict(
        (name, Endpoint(hostname, port)) for (name, port) in portmap.items()
        if isinstance(port, int))

    # It's possible for the primary port to not have been allocated if this task
    # is using autoregistration, so register with a port of 0.
    return Endpoint(hostname, portmap.get(primary_port, 0)), additional_endpoints

  @classmethod
  def super_credentials(cls):
    try:
      with open(cls.DEFAULT_ACL_PATH) as fp:
        super_creds = fp.read().rstrip().split(':', 1)
        if len(super_creds) != 2:
          log.error('Bad ACL, expected format of super:credentials')
          return
        user, blob = super_creds
        if user == 'super':
          return ('digest', 'super:%s' % blob)
        else:
          log.error('Bad ACL, expected super user, got %s' % user)
    except (OSError, IOError) as e:
      log.warning('Failed to open zookeeper ACL file: %s' % e)

  def __init__(self, role,
                     environment,
                     jobname,
                     hostname,
                     primary_port,
                     portmap,
                     shard,
                     ensemble=None):
    self._unhealthy = threading.Event()

    try:
      primary, additional = self.join_keywords(hostname, portmap, primary_port)
    except ValueError:
      self._service = None
      self._unhealthy.set()
    else:
      self._service = TwitterService(
          role,
          environment,
          jobname,
          primary,
          additional=additional,
          failure_callback=self.on_failure,
          shard=shard,
          ensemble=ensemble)
          # TODO(wickman) This is disabled until MESOS-2376 is resolved.
          # credentials=self.super_credentials())

  def on_failure(self):
    if self._service:
      self._service.rejoin()

  @property
  def healthy(self):
    return not self._unhealthy.is_set()

  def stop(self):
    if self._service:
      self._service.cancel()
