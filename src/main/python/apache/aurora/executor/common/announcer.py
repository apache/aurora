#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import posixpath
import threading
import time
from abc import abstractmethod

from kazoo.client import KazooClient
from kazoo.retry import KazooRetry
from mesos.interface import mesos_pb2
from twitter.common import app, log
from twitter.common.concurrent.deferred import defer
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import LambdaGauge, Observable
from twitter.common.quantity import Amount, Time
from twitter.common.zookeeper.serverset import Endpoint, ServerSet

from apache.aurora.executor.common.status_checker import (
    StatusChecker,
    StatusCheckerProvider,
    StatusResult
)
from apache.aurora.executor.common.task_info import (
    mesos_task_instance_from_assigned_task,
    resolve_ports
)


def make_endpoints(hostname, portmap, primary_port):
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


class AnnouncerCheckerProvider(StatusCheckerProvider):
  def __init__(self, allow_custom_serverset_path=False, name=None):
    self.name = name
    self.__allow_custom_serverset_path = allow_custom_serverset_path
    super(AnnouncerCheckerProvider, self).__init__()

  @abstractmethod
  def make_zk_client(self):
    """Create a ZooKeeper client which can be asyncronously started"""

  @abstractmethod
  def make_zk_path(self, assigned_task):
    """Given an assigned task return the path into where we should announce the task."""

  def from_assigned_task(self, assigned_task, _):
    mesos_task = mesos_task_instance_from_assigned_task(assigned_task)

    if not mesos_task.has_announce():
      return None

    portmap = resolve_ports(mesos_task, assigned_task.assignedPorts)

    # assigned_task.slaveHost is the --hostname argument passed into the mesos slave.
    # Using this allows overriding the hostname published into ZK when announcing.
    # If no argument was passed to the mesos-slave, the slave falls back to gethostname().
    endpoint, additional = make_endpoints(
      assigned_task.slaveHost,
      portmap,
      mesos_task.announce().primary_port().get())

    client = self.make_zk_client()
    if mesos_task.announce().has_zk_path():
      if self.__allow_custom_serverset_path:
        path = mesos_task.announce().zk_path().get()
      else:
        app.error('Executor must be started with --announcer-allow-custom-serverset-path in order '
            'to use zk_path in the Announcer config')
    else:
      path = self.make_zk_path(assigned_task)

    initial_interval = mesos_task.health_check_config().initial_interval_secs().get()
    interval = mesos_task.health_check_config().interval_secs().get()
    consecutive_failures = mesos_task.health_check_config().max_consecutive_failures().get()
    timeout_secs = initial_interval + (consecutive_failures * interval)

    return AnnouncerChecker(
      client, path, timeout_secs, endpoint, additional=additional, shard=assigned_task.instanceId,
      name=self.name)


class DefaultAnnouncerCheckerProvider(AnnouncerCheckerProvider):
  DEFAULT_RETRY_MAX_DELAY = Amount(5, Time.MINUTES)
  DEFAULT_RETRY_POLICY = KazooRetry(
      max_tries=None,
      ignore_expire=True,
      max_delay=DEFAULT_RETRY_MAX_DELAY.as_(Time.SECONDS),
  )

  def __init__(self, ensemble, root='/aurora', allow_custom_serverset_path=False):
    self.__ensemble = ensemble
    self.__root = root
    super(DefaultAnnouncerCheckerProvider, self).__init__(allow_custom_serverset_path)

  def make_zk_client(self):
    return KazooClient(self.__ensemble, connection_retry=self.DEFAULT_RETRY_POLICY)

  def make_zk_path(self, assigned_task):
    config = assigned_task.task
    role, environment, name = (config.job.role, config.job.environment, config.job.name)
    return posixpath.join(self.__root, role, environment, name)


class ServerSetJoinThread(ExceptionalThread):
  """Background thread to reconnect to Serverset on session expiration."""

  LOOP_WAIT = Amount(1, Time.SECONDS)

  def __init__(self, event, joiner, loop_wait=LOOP_WAIT):
    self._event = event
    self._joiner = joiner
    self._stopped = threading.Event()
    self._loop_wait = loop_wait
    super(ServerSetJoinThread, self).__init__()
    self.daemon = True

  def run(self):
    while True:
      if self._stopped.is_set():
        break
      self._event.wait(timeout=self._loop_wait.as_(Time.SECONDS))
      if not self._event.is_set():
        continue
      log.debug('Join event triggered, joining serverset.')
      self._event.clear()
      self._joiner()

  def stop(self):
    self._stopped.set()


class Announcer(Observable):
  class Error(Exception): pass

  EXCEPTION_WAIT = Amount(15, Time.SECONDS)

  def __init__(self,
               serverset,
               endpoint,
               additional=None,
               shard=None,
               clock=time,
               exception_wait=None):
    self._membership = None
    self._membership_termination = clock.time()
    self._endpoint = endpoint
    self._additional = additional or {}
    self._shard = shard
    self._serverset = serverset
    self._rejoin_event = threading.Event()
    self._clock = clock
    self._thread = None
    self._exception_wait = exception_wait or self.EXCEPTION_WAIT

  def disconnected_time(self):
    # Lockless membership length check
    membership_termination = self._membership_termination
    if membership_termination is None:
      return 0
    return self._clock.time() - membership_termination

  def _join_inner(self):
    return self._serverset.join(
        endpoint=self._endpoint,
        additional=self._additional,
        shard=self._shard,
        expire_callback=self.on_expiration)

  def _join(self):
    if self._membership is not None:
      raise self.Error("join called, but already have membership!")
    while True:
      try:
        self._membership = self._join_inner()
        self._membership_termination = None
      except Exception as e:
        log.error('Failed to join ServerSet: %s' % e)
        self._clock.sleep(self._exception_wait.as_(Time.SECONDS))
      else:
        break

  def start(self):
    self._thread = ServerSetJoinThread(self._rejoin_event, self._join)
    self._thread.start()
    self.rejoin()

  def rejoin(self):
    self._rejoin_event.set()

  def stop(self):
    thread, self._thread = self._thread, None
    thread.stop()
    if self._membership:
      self._serverset.cancel(self._membership)

  def on_expiration(self):
    self._membership = None
    if not self._thread:
      return
    self._membership_termination = self._clock.time()
    log.info('Zookeeper session expired.')
    self.rejoin()


class AnnouncerChecker(StatusChecker):
  DEFAULT_NAME = 'announcer'

  def __init__(self, client, path, timeout_secs, endpoint, additional=None, shard=None, name=None):
    self.__client = client
    self.__connect_event = client.start_async()
    self.__timeout_secs = timeout_secs
    self.__announcer = Announcer(ServerSet(client, path), endpoint, additional=additional,
        shard=shard)
    self.__name = name or self.DEFAULT_NAME
    self.__status = None
    self.start_event = threading.Event()
    self.metrics.register(LambdaGauge('disconnected_time', self.__announcer.disconnected_time))

  @property
  def status(self):
    return self.__status

  def name(self):
    return self.__name

  def __start(self):
    self.__connect_event.wait(timeout=self.__timeout_secs)
    if not self.__connect_event.is_set():
      self.__status = StatusResult("Creating Announcer Serverset timed out.", mesos_pb2.TASK_FAILED)
    else:
      self.__announcer.start()

    self.start_event.set()

  def start(self):
    defer(self.__start)

  def stop(self):
    defer(self.__announcer.stop)
