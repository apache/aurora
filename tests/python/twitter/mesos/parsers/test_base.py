from twitter.mesos.parsers.base import PortResolver

import pytest


resolve = PortResolver.resolve
unallocated = PortResolver.unallocated
bound = PortResolver.bound


def test_all_static():
  portmap = {}
  assert resolve(portmap) == {}
  assert bound(portmap) == set()
  assert unallocated(portmap) == set()

  portmap = {'port': '80'}
  assert resolve(portmap) == {'port': 80}
  assert bound(resolve(portmap)) == set(['port'])
  assert unallocated(resolve(portmap)) == set()


def test_binding():
  portmap = {'aurora': 'http', 'http': 80}
  assert resolve(portmap) == {'aurora': 80, 'http': 80}
  assert unallocated(resolve(portmap)) == set()
  assert bound(resolve(portmap)) == set(['aurora', 'http'])

  portmap = {'aurora': 'http', 'http': 'unbound'}
  assert resolve(portmap) == {'aurora': 'unbound', 'http': 'unbound'}
  assert unallocated(resolve(portmap)) == set(['unbound'])
  assert bound(resolve(portmap)) == set(['aurora', 'http'])


def test_cycle():
  portmap = {'aurora': 'http', 'http': 'aurora'}
  with pytest.raises(PortResolver.CycleException):
    resolve(portmap)
  portmap = {'aurora': 'http', 'http': 'https', 'https': 'aurora'}
  with pytest.raises(PortResolver.CycleException):
    resolve(portmap)

