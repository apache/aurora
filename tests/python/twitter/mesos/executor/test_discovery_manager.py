import getpass
import pytest
import socket
import threading

from twitter.common import log
from twitter.common.log.options import LogOptions

from twitter.common.contextutil import temporary_dir
from twitter.common.net.tunnel import TunnelHelper
from twitter.common.zookeeper.client import ZooKeeper
from twitter.common.zookeeper.serverset import ServerSet, Endpoint
from twitter.common.zookeeper.test_server import ZookeeperServer
from twitter.common_internal.zookeeper.twitter_service import TwitterService
from twitter.mesos.config.schema import (
  MesosTaskInstance,
  Announcer,
  Task,
  Process,
  Resources)
from twitter.mesos.executor.discovery_manager import DiscoveryManager


def hello_world(announce=False, **kw):
  mti = MesosTaskInstance(
    task = Task(name = 'hello_world',
                processes = [
                  Process(name = 'hello_world', cmdline = 'echo hello world')
                ],
                resources = Resources(cpu=1.0, ram=1024, disk=1024)),
    instance = 0,
    role = getpass.getuser())

  return mti(announce=Announcer(**kw)) if announce else mti


class TestDiscoveryManager(object):
  ZKSERVER = None
  ZK = None

  @classmethod
  def setup_class(cls):
    cls.ZKSERVER = ZookeeperServer()
    cls.ZK = ZooKeeper(cls.ZKSERVER.ensemble,
         authentication=('digest', '%(user)s:%(user)s' % {'user': getpass.getuser()}))

  @classmethod
  def teardown_class(cls):
    cls.ZKSERVER.stop()
    cls.ZK.close()

  def test_assertions(self):
    # Must be constructed with an announce object
    with pytest.raises(AssertionError):
      DiscoveryManager(hello_world(), {}, 0)

    dm = DiscoveryManager(hello_world(announce=True), {}, 0)
    assert not dm.healthy

    dm = DiscoveryManager(hello_world(announce=True), {'poop': 1234}, 0)
    assert not dm.healthy

  @classmethod
  def make_ss(cls, task, **kw):
    return ServerSet(cls.ZK, TwitterService.zkpath(
        task.role(), task.announce().environment(), task.task().name()), **kw)

  def _make_manager(self, task, portmap, shard):
    dm = DiscoveryManager(task, portmap, shard, ensemble=self.ZKSERVER.ensemble)
    join_event = threading.Event()
    exit_event = threading.Event()
    def on_join(_):
      join_event.set()
    def on_exit(_):
      exit_event.set()
    ss = self.make_ss(task, on_join=on_join, on_leave=on_exit)
    return (dm, join_event, exit_event, ss)

  def test_basic_registration(self):
    portmap = {'http': TunnelHelper.get_random_port()}
    task = hello_world(announce=True, primary_port='http')
    dm, join_event, exit_event, ss = self._make_manager(task, portmap, 23)

    try:
      join_event.wait(timeout=1.0)
      assert join_event.is_set()

      instances = list(ss)
      assert len(instances) == 1

      instance = instances[0]
      assert instance.service_endpoint == Endpoint(socket.gethostname(), portmap['http'])
      assert instance.shard == 23

      assert dm.healthy
    finally:
      dm.stop()

    exit_event.wait(timeout=1.0)
    assert exit_event.is_set()

  def _make_manager_and_cancel(self, task, portmap, shard, assertion_callback):
    dm, join_event, exit_event, ss = self._make_manager(task, portmap, shard)

    try:
      join_event.wait(timeout=1.0)
      assert join_event.is_set()
      assert dm.healthy

      members = ss._group.list()
      assert len(members) == 1
      ss.cancel(members[0])

      assertion_callback(join_event, exit_event, dm)
    finally:
      dm.stop()

  def test_strict_on(self):
    def assertion_callback(join_event, exit_event, disco_manager):
      exit_event.wait(timeout=1.0)
      assert exit_event.is_set()
      disco_manager._unhealthy.wait(timeout=1.0)
      assert not disco_manager.healthy
    self._make_manager_and_cancel(hello_world(announce=True, primary_port='http', strict=True),
                                  {'http': TunnelHelper.get_random_port()},
                                  43,
                                  assertion_callback)

  def test_strict_off(self):
    def assertion_callback(join_event, exit_event, disco_manager):
      exit_event.wait(timeout=1.0)
      assert exit_event.is_set()
      join_event.wait(timeout=1.0)
      assert join_event.is_set()
      assert disco_manager.healthy
    self._make_manager_and_cancel(hello_world(announce=True, primary_port='http', strict=False),
                                  {'http': TunnelHelper.get_random_port()},
                                  17,
                                  assertion_callback)
