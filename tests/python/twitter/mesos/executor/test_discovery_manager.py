import getpass
import os
import pytest
import threading

from twitter.common import log
from twitter.common.log.options import LogOptions

from twitter.common.contextutil import temporary_dir
from twitter.common.net.tunnel import TunnelHelper
from twitter.common.zookeeper.client import ZooKeeper
from twitter.common.zookeeper.serverset import ServerSet, Endpoint
from twitter.common.zookeeper.test_server import ZookeeperServer
from twitter.common_internal.zookeeper.twitter_serverset import TwitterServerSet
from twitter.mesos.executor.discovery_manager import DiscoveryManager


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
  def make_ss(cls, role, environment, jobname, **kw):
    return ServerSet(cls.ZK, TwitterServerSet.zkpath(role, environment, jobname), **kw)

  def _make_manager(self, *args):
    role, environment, jobname = args[0:3]
    dm = DiscoveryManager(*args, ensemble=self.ZKSERVER.ensemble)
    join_event = threading.Event()
    exit_event = threading.Event()
    def on_join(_):
      join_event.set()
    def on_exit(_):
      exit_event.set()
    ss = self.make_ss(role, environment, jobname, on_join=on_join, on_leave=on_exit)
    return (dm, join_event, exit_event, ss)

  def test_basic_registration(self):
    me = 'foo.bar.baz.com'
    portmap = {'http': TunnelHelper.get_random_port(), 'bar': TunnelHelper.get_random_port()}
    dm, join_event, exit_event, ss = self._make_manager(
        getpass.getuser(), 'devel', 'my_job', me, 'http', portmap, 23)

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
    me = 'foo.bar.baz.com'
    dm, join_event, exit_event, ss = self._make_manager(
        getpass.getuser(), 'prod', 'my_job', me, 'http', {'http': TunnelHelper.get_random_port()},
        17)

    try:
      join_event.wait(timeout=10.0)
      assert join_event.is_set()
      assert dm.healthy

      members = ss._group.list()
      assert len(members) == 1
      old_instance = list(ss)[0]
      ss.cancel(members[0])

      exit_event.wait(timeout=10.0)
      assert exit_event.is_set()

      join_event.wait(timeout=10.0)
      assert join_event.is_set()
      instances = list(ss)
      assert len(instances) > 0
      assert instances[0] == old_instance

      assert dm.healthy

    finally:
      dm.stop()


def test_acls():
  with temporary_dir() as td:
    class AltDiscoveryManager(DiscoveryManager):
      DEFAULT_ACL_PATH = os.path.join(td, 'service.yml')

    assert AltDiscoveryManager.super_credentials() == None

    def expected_creds(data):
      with open(os.path.join(td, 'service.yml'), 'wb') as fp:
        fp.write(data)
      return AltDiscoveryManager.super_credentials()

    assert expected_creds('') is None
    assert expected_creds('dingdong') is None
    assert expected_creds('super') is None
    assert expected_creds('super:') == ('digest', 'super:')
    assert expected_creds('super::') == ('digest', 'super::')
    assert expected_creds('frank:gore') is None
    assert expected_creds('super:dup:er:') == ('digest', 'super:dup:er:')
