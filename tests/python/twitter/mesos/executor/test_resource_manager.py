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
  assert isinstance(kr, ResourceEnforcer.KillReason)
  assert kr._reason.startswith('RAM')

  # disk
  assert enf.enforce(TRS(reservedDiskBytes=2, diskBytes=1)) is None
  kr = enf.enforce(TRS(reservedDiskBytes=1, diskBytes=2))
  assert isinstance(kr, ResourceEnforcer.KillReason)
  assert kr._reason.startswith('Disk')

  # ordered
  kr = enf.enforce(
      TRS(reservedRamBytes=2, ramRssBytes=3,
          reservedDiskBytes=3, diskBytes=2))
  assert isinstance(kr, ResourceEnforcer.KillReason) and kr._reason.startswith('RAM')
  kr = enf.enforce(
      TRS(reservedRamBytes=3, ramRssBytes=2,
          reservedDiskBytes=2, diskBytes=3))
  assert isinstance(kr, ResourceEnforcer.KillReason) and kr._reason.startswith('Disk')
