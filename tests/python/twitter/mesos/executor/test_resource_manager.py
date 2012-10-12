import random

from twitter.mesos.executor.health_interface import FailureReason
from twitter.mesos.executor.resource_manager import ResourceEnforcer
from gen.twitter.mesos.comm.ttypes import TaskResourceSample


def test_resource_enforcer():
  TRS = TaskResourceSample
  enf = ResourceEnforcer(pid=-1)

  # cpu only niced
  assert enf.enforce(TRS(reservedCpuRate=1, cpuRate=0.9)) is None
  assert enf.enforce(TRS(reservedCpuRate=0.9, cpuRate=1)) is None

  # ram
  for (k, v) in {'ramRssBytes': 1, 'ramRssBytes': 2, 'ramVssBytes': 1, 'ramVssBytes': 3}.items():
    assert enf.enforce(TRS(reservedRamBytes=2, **{k:v})) is None
  kr = enf.enforce(TRS(reservedRamBytes=2, ramRssBytes=3))
  assert isinstance(kr, FailureReason)
  assert kr.reason.startswith('RAM')

  # disk
  assert enf.enforce(TRS(reservedDiskBytes=2, diskBytes=1)) is None
  kr = enf.enforce(TRS(reservedDiskBytes=1, diskBytes=2))
  assert isinstance(kr, FailureReason)
  assert kr.reason.startswith('Disk')

  # ordered
  kr = enf.enforce(
      TRS(reservedRamBytes=2, ramRssBytes=3,
          reservedDiskBytes=3, diskBytes=2))
  assert isinstance(kr, FailureReason) and kr.reason.startswith('RAM')
  kr = enf.enforce(
      TRS(reservedRamBytes=3, ramRssBytes=2,
          reservedDiskBytes=2, diskBytes=3))
  assert isinstance(kr, FailureReason) and kr.reason.startswith('Disk')


class FakeResourceEnforcer(ResourceEnforcer):
  def __init__(self, portmap={}, listening_ports=frozenset()):
    super(FakeResourceEnforcer, self).__init__(-1, portmap=portmap)
    self._listening_ports = listening_ports

  def get_listening_ports(self):
    return iter(self._listening_ports)

  def enforce_ports(self):
    return super(FakeResourceEnforcer, self)._enforce_ports(None)


def test_port_enforcement():
  def randrange():
    return random.randrange(*FakeResourceEnforcer.ENFORCE_PORT_RANGE)

  # basic cases
  fre = FakeResourceEnforcer(portmap={})
  assert fre.enforce_ports() is None
  fre = FakeResourceEnforcer(portmap={'http': randrange()})
  assert fre.enforce_ports() is None
  port = randrange()
  fre = FakeResourceEnforcer(portmap={'http': port}, listening_ports=(port,))
  assert fre.enforce_ports() is None

  # edge cases
  edge0, edge1 = FakeResourceEnforcer.ENFORCE_PORT_RANGE
  fre = FakeResourceEnforcer(portmap={'http': edge0}, listening_ports=(edge0,))
  assert fre.enforce_ports() is None
  fre = FakeResourceEnforcer(portmap={'http': edge1}, listening_ports=(edge1,))
  assert fre.enforce_ports() is None
  fre = FakeResourceEnforcer(listening_ports=(edge1+1,))
  assert fre.enforce_ports() is None
  fre = FakeResourceEnforcer(listening_ports=(edge0-1,))
  assert fre.enforce_ports() is None
  fre = FakeResourceEnforcer(portmap={'http': edge0}, listening_ports=(edge1,))
  assert fre.enforce_ports() is not None
  fre = FakeResourceEnforcer(portmap={'http': edge1}, listening_ports=(edge0,))
  assert fre.enforce_ports() is not None
