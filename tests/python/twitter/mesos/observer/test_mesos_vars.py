from collections import namedtuple

from twitter.common.contextutil import temporary_file, open_zip
from twitter.mesos.observer.mesos_vars import (
    ExecutorVars,
    MesosObserverVars)

import mox
import psutil

EV = ExecutorVars
MOV = MesosObserverVars


def test_release_from_tag():
  unknown_tags = (
    '', 'thermos_0', 'thermos_executor_0', 'thermos_0.2.3', 'wat', 'asdfasdfasdf',
    'thermos-r32', 'thermos_r32')
  for tag in unknown_tags:
    assert EV.get_release_from_tag(tag) == 'UNKNOWN'

  assert EV.get_release_from_tag('thermos_R0') == 0
  assert EV.get_release_from_tag('thermos_R32') == 32
  assert EV.get_release_from_tag('thermos_executor_R12') == 12
  assert EV.get_release_from_tag('thermos_smf1-test_16_R32') == 16
  assert EV.get_release_from_tag('thermos_executor_smf1-test_23_R10') == 23


def test_extract_pexinfo():
  filename = None
  with temporary_file() as fp:
    filename = fp.name
    with open_zip(filename, 'w') as zf:
      zf.writestr('PEX-INFO', '{"build_properties":{"tag":"thermos_R31337"}}')
    assert EV.get_release_from_binary(filename) == 31337
  assert EV.get_release_from_binary(filename) == 'UNKNOWN'
  assert EV.get_release_from_binary('lololololo') == 'UNKNOWN'


class FakeProcess(object):
  def __init__(self, cwd, cmdline, raises=None):
    self._cwd = cwd
    self._cmdline = cmdline
    self._raise = raises

  def getcwd(self):
    if self._raise:
      raise self._raise
    return self._cwd

  @property
  def cmdline(self):
    if self._raise:
      raise self._raise
    return self._cmdline


NON_EXECUTORS = [
    FakeProcess(None, []),
    FakeProcess(None, ['too short']),
    FakeProcess(None, ['too short']),
    FakeProcess(None, ['just', 'wrong']),
    FakeProcess(None, ['python', './gc_executor']),
]

EXECUTORS = [
    FakeProcess(None, ['python', './thermos_executor']),
    FakeProcess(None, ['python', './thermos_executor.pex']),
    FakeProcess(None, ['python2', './thermos_executor']),
    FakeProcess(None, ['python2.6', './thermos_executor.pex']),
]

BAD_EXECUTORS = [
    FakeProcess(None, [], raises=psutil.error.Error('No such process')),
    FakeProcess(None, ['python', './thermos_executor.pex'], raises=psutil.error.Error('Lulz')),
]

def test_iter_executors():
  m = mox.Mox()
  m.StubOutWithMock(psutil, 'process_iter')
  psutil.process_iter().AndRaise(psutil.error.Error('derp'))
  m.ReplayAll()
  assert list(MOV.iter_executors()) == []
  m.UnsetStubs()
  m.VerifyAll()

  m = mox.Mox()
  m.StubOutWithMock(psutil, 'process_iter')
  psutil.process_iter().AndReturn(iter(EXECUTORS))
  psutil.process_iter().AndReturn(iter(NON_EXECUTORS + EXECUTORS))
  psutil.process_iter().AndReturn(iter(BAD_EXECUTORS + EXECUTORS))
  psutil.process_iter().AndReturn(iter(EXECUTORS + NON_EXECUTORS))
  psutil.process_iter().AndReturn(iter(EXECUTORS + BAD_EXECUTORS))
  psutil.process_iter().AndReturn(iter(NON_EXECUTORS + EXECUTORS + NON_EXECUTORS))
  psutil.process_iter().AndReturn(iter(BAD_EXECUTORS + EXECUTORS + BAD_EXECUTORS))
  m.ReplayAll()
  for _ in range(7):
    assert list(MOV.iter_executors()) == EXECUTORS
  m.UnsetStubs()
  m.VerifyAll()
