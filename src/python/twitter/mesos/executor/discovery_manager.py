import socket
import threading

from twitter.common import log
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset import Endpoint
from twitter.common_internal.zookeeper.twitter_service import TwitterService

from .health_interface import HealthInterface


class DiscoveryManager(HealthInterface):
  @staticmethod
  def join_keywords(portmap, primary_port):
    """
      Generate primary, additional endpoints from a portmap and primary_port.
      primary_port must be a name in the portmap dictionary.
    """
    assert primary_port in portmap
    hostname = socket.gethostname()
    return Endpoint(hostname, portmap[primary_port]), dict(
        ((port, Endpoint(hostname, portmap[port])) for port in portmap))

  def __init__(self, task, portmap, ensemble=None):
    assert task.has_announce()
    announce_config = task.announce()
    primary, additional = self.join_keywords(portmap, announce_config.primary_port().get())
    self._strict = bool(announce_config.strict().get())
    self._service = TwitterService(
        task.role().get(),
        announce_config.environment().get(),
        task.task().name(),
        primary,
        additional=additional,
        strict=True,
        failure_callback=self.on_failure,
        ensemble=ensemble)
    self._unhealthy = threading.Event()

  def on_failure(self):
    if self._strict:
      self._unhealthy.set()
    else:
      self._service.rejoin()

  @property
  def healthy(self):
    return not self._unhealthy.is_set()

  def stop(self):
    self._service.cancel()
