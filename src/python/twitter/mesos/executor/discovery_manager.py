import socket
import threading

from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset import Endpoint
from twitter.common_internal.zookeeper.twitter_service import TwitterService

from .health_interface import HealthInterface


class DiscoveryManager(HealthInterface):
  DEFAULT_ACL_ROLE = 'mesos'

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

  def __init__(self, task, hostname, portmap, shard, ensemble=None):
    assert task.has_announce()
    announce_config = task.announce()
    self._unhealthy = threading.Event()

    try:
      primary, additional = self.join_keywords(
          hostname,
          portmap,
          announce_config.primary_port().get())
    except ValueError:
      self._service = None
      self._unhealthy.set()
    else:
      self._service = TwitterService(
          task.role().get(),
          task.environment().get() if task.has_environment() else 'devel',
          task.task().name(),
          primary,
          additional=additional,
          failure_callback=self.on_failure,
          shard=shard,
          ensemble=ensemble)

  def on_failure(self):
    if self._service:
      self._service.rejoin()

  @property
  def healthy(self):
    return not self._unhealthy.is_set()

  def stop(self):
    if self._service:
      self._service.cancel()
