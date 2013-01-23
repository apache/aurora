import socket
import threading

from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset import Endpoint
from twitter.common_internal.zookeeper.twitter_service import TwitterService

from .health_interface import HealthInterface


class DiscoveryManager(HealthInterface):
  @staticmethod
  def join_keywords(hostname, portmap, primary_port, stats_port):
    """
      Generate primary, additional endpoints from a portmap and primary_port.
      primary_port must be a name in the portmap dictionary.
    """
    if primary_port not in portmap:
      raise ValueError('Cannot create Endpoint if primary port is not in portmap!')
    if stats_port not in portmap:
      raise ValueError('Cannot create Endpoint if stats port is not in portmap!')
    additional_endpoints = dict(
        (port, Endpoint(hostname, portmap[port])) for port in portmap)
    additional_endpoints.update(aurora=Endpoint(hostname, portmap[stats_port]))
    return Endpoint(hostname, portmap[primary_port]), additional_endpoints

  def __init__(self, task, hostname, portmap, shard, ensemble=None):
    assert task.has_announce()
    announce_config = task.announce()
    self._strict = bool(announce_config.strict().get())
    self._unhealthy = threading.Event()

    try:
      primary, additional = self.join_keywords(
          hostname,
          portmap,
          announce_config.primary_port().get(),
          announce_config.stats_port().get())
    except ValueError:
      self._service = None
      self._unhealthy.set()
    else:
      self._service = TwitterService(
          task.role().get(),
          announce_config.environment().get(),
          task.task().name(),
          primary,
          additional=additional,
          strict=True,
          failure_callback=self.on_failure,
          shard=shard,
          ensemble=ensemble)

  def on_failure(self):
    if self._strict:
      self._unhealthy.set()
    else:
      if self._service:
        self._service.rejoin()

  @property
  def healthy(self):
    return not self._unhealthy.is_set()

  def stop(self):
    if self._service:
      self._service.cancel()
