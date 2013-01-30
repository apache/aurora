import getpass
import pytest
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
         authentication=('digest', '%(user)s:%(user)s' % {
            'user': DiscoveryManager.DEFAULT_ACL_ROLE}))

  @classmethod
  def teardown_class(cls):
    cls.ZKSERVER.stop()
    cls.ZK.close()

  def test_assertions(self):
    # Must be constructed with an announce object
    with pytest.raises(AssertionError):
      DiscoveryManager(hello_world(), 'asdf', {}, 0)

  def test_join_keywords(self):
    me = 'my.host.name'

    primary, additional = DiscoveryManager.join_keywords(me, {}, 'http')
    assert primary == Endpoint(me, 0)
    assert additional == {}

    primary, additional = DiscoveryManager.join_keywords(me, {'http': 80}, 'http')
    assert primary == Endpoint(me, 80)
    assert additional == {'http': Endpoint(me, 80)}

    primary, additional = DiscoveryManager.join_keywords(me, {'blah': 80}, 'http')
    assert primary == Endpoint(me, 0)
    assert additional == {'blah': Endpoint(me, 80)}

    primary, additional = DiscoveryManager.join_keywords(me, {'http': 80, 'blah': 8080}, 'http')
    assert primary == Endpoint(me, 80)
    assert additional == {
      'http': Endpoint(me, 80),
      'blah': Endpoint(me, 8080),
    }

  @classmethod
  def make_ss(cls, task, **kw):
    return ServerSet(cls.ZK, TwitterService.zkpath(
        task.role().get(),
        task.environment() if task.has_environment() else 'devel',
        task.task().name()),
        **kw)

  def _make_manager(self, task, host, portmap, shard):
    dm = DiscoveryManager(task, host, portmap, shard, ensemble=self.ZKSERVER.ensemble)
    join_event = threading.Event()
    exit_event = threading.Event()
    def on_join(_):
      join_event.set()
    def on_exit(_):
      exit_event.set()
    ss = self.make_ss(task, on_join=on_join, on_leave=on_exit)
    return (dm, join_event, exit_event, ss)

  def test_basic_registration(self):
    me = 'foo.bar.baz'
    portmap = {'http': TunnelHelper.get_random_port(), 'bar': TunnelHelper.get_random_port()}
    task = hello_world(announce=True, primary_port='http')
    dm, join_event, exit_event, ss = self._make_manager(task, me, portmap, 23)

    try:
      join_event.wait(timeout=1.0)
      assert join_event.is_set()

      instances = list(ss)
      assert len(instances) == 1

      instance = instances[0]
      assert instance.service_endpoint == Endpoint(me, portmap['http'])
      assert instance.additional_endpoints == {
        'http': Endpoint(me, portmap['http']),
        'bar': Endpoint(me, portmap['bar']),
      }
      assert instance.shard == 23

      assert dm.healthy
    finally:
      dm.stop()

    exit_event.wait(timeout=1.0)
    assert exit_event.is_set()

  def test_rejoin(self):
    task = hello_world(announce=True, primary_port='http')
    dm, join_event, exit_event, ss = self._make_manager(task, 'asdf',
        {'http': TunnelHelper.get_random_port()}, 17)

    try:
      join_event.wait(timeout=10.0)
      assert join_event.is_set()
      assert dm.healthy

      members = ss._group.list()
      assert len(members) == 1
      ss.cancel(members[0])

      exit_event.wait(timeout=10.0)
      assert exit_event.is_set()

      join_event.wait(timeout=10.0)
      assert join_event.is_set()

      assert dm.healthy

    finally:
      dm.stop()
