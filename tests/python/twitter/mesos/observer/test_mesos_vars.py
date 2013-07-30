from collections import namedtuple

from twitter.common.contextutil import temporary_file, open_zip
from twitter.mesos.executor.executor_detector import ExecutorDetector
from twitter.mesos.observer.mesos_vars import (
    ThermosExecutorVars as EV,
    MesosObserverVars as MOV)

import mox
import psutil


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
    self.cwd = cwd
    self._cmdline = cmdline
    self.raises = raises

  def getcwd(self):
    if self.raises:
      raise self.raises
    return self.cwd

  @property
  def cmdline(self):
    if self.raises:
      raise self.raises
    return self._cmdline


NON_EXECUTORS = [
    FakeProcess(True, []),
    FakeProcess(True, ['too short']),
    FakeProcess(True, ['too short']),
    FakeProcess(True, ['just', 'wrong']),
    FakeProcess(True, ['python', './gc_executor']),
]

BAD_EXECUTORS = [
    FakeProcess(True, [], raises=psutil.Error('No such process')),
    FakeProcess(True, ['python', './thermos_executor.pex'], raises=psutil.Error('Lulz')),
]

GOOD_EXECUTORS = [
    FakeProcess(True, ['python', './thermos_executor']),
    FakeProcess(True, ['python', './thermos_executor.pex']),
    FakeProcess(True, ['python2', './thermos_executor']),
    FakeProcess(True, ['python2.6', './thermos_executor.pex']),
]

SANDBOXLESS_EXECUTORS = [
    FakeProcess(None, ['python', './thermos_executor']),
    FakeProcess(None, ['python', './thermos_executor.pex'])
]

def test_iter_executors_psutil_fail():
  m = mox.Mox()
  m.StubOutWithMock(psutil, 'process_iter')
  psutil.process_iter().AndRaise(psutil.Error('derp'))
  m.ReplayAll()
  assert list(MOV.iter_executors()) == []
  m.UnsetStubs()
  m.VerifyAll()

def test_iter_executors():
  m = mox.Mox()
  m.StubOutWithMock(psutil, 'process_iter')
  m.StubOutWithMock(ExecutorDetector, 'match')

  def mock_executor_set(execs):
    psutil.process_iter().AndReturn(iter(execs))
    for e in execs:
      if e in GOOD_EXECUTORS + SANDBOXLESS_EXECUTORS:
        ExecutorDetector.match(e.cwd).AndReturn(e.cwd)

  mock_executor_set(GOOD_EXECUTORS)
  mock_executor_set(NON_EXECUTORS + GOOD_EXECUTORS)
  mock_executor_set(BAD_EXECUTORS + GOOD_EXECUTORS)
  mock_executor_set(GOOD_EXECUTORS + NON_EXECUTORS)
  mock_executor_set(GOOD_EXECUTORS + BAD_EXECUTORS)
  mock_executor_set(NON_EXECUTORS + GOOD_EXECUTORS + NON_EXECUTORS)
  mock_executor_set(BAD_EXECUTORS + GOOD_EXECUTORS + BAD_EXECUTORS)

  m.ReplayAll()

  for _ in range(7):
    assert list(p for p, _ in MOV.iter_executors()) == GOOD_EXECUTORS
  m.UnsetStubs()
  m.VerifyAll()
