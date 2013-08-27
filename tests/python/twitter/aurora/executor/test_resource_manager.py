import random

from twitter.aurora.config.schema import Resources as _Resources
from twitter.aurora.executor.health_interface import FailureReason
from twitter.aurora.executor.resource_manager import ResourceEnforcer
from twitter.thermos.base.path import TaskPath

from gen.twitter.aurora.comm.ttypes import TaskResourceSample


class DummyTaskMonitor(object):
  def get_active_processes(self):
    return
    yield

Resources = _Resources(cpu=0, ram=0, disk=0)

def resource_enforcer_maker(**kw):
  return ResourceEnforcer(
    resources=Resources(**kw),
    task_monitor=DummyTaskMonitor(),
    portmap={}
  )

def test_resource_enforcer():
  TRS = TaskResourceSample
  RE = resource_enforcer_maker

  # cpu only niced
  assert RE(cpu=1).enforce(TRS(cpuRate=0.9)) is None
  assert RE(cpu=0.9).enforce(TRS(cpuRate=1)) is None

  # ram
  for (k, v) in {'ramRssBytes': 1, 'ramRssBytes': 2, 'ramVssBytes': 1, 'ramVssBytes': 3}.items():
    assert RE(ram=2).enforce(TRS(**{k:v})) is None
  kr = RE(ram=2).enforce(TRS(ramRssBytes=3))
  assert isinstance(kr, FailureReason)
  assert kr.reason.startswith('RAM')

  # disk
  assert RE(disk=2).enforce(TRS(diskBytes=1)) is None
  kr = RE(disk=1).enforce(TRS(diskBytes=2))
  assert isinstance(kr, FailureReason)
  assert kr.reason.startswith('Disk')

  # ordered
  kr = RE(ram=2, disk=3).enforce(TRS(ramRssBytes=3, diskBytes=2))
  assert isinstance(kr, FailureReason) and kr.reason.startswith('RAM')
  kr = RE(ram=3, disk=2).enforce(TRS(ramRssBytes=2, diskBytes=3))
  assert isinstance(kr, FailureReason) and kr.reason.startswith('Disk')


class FakeResourceEnforcer(ResourceEnforcer):
  def __init__(self, portmap={}, listening_ports=frozenset()):
    super(FakeResourceEnforcer, self).__init__(
        resources=Resources(), task_monitor=DummyTaskMonitor(), portmap=portmap)
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
