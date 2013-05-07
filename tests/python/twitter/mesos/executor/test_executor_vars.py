from twitter.common.contextutil import temporary_file, open_zip

from twitter.mesos.executor.executor_vars import ExecutorVars


def test_release_from_tag():
  unknown_tags = (
    '', 'thermos_0', 'thermos_executor_0', 'thermos_0.2.3', 'wat', 'asdfasdfasdf',
    'thermos-r32', 'thermos_r32')
  for tag in unknown_tags:
    assert ExecutorVars.get_release_from_tag(tag) == 'UNKNOWN'
  assert ExecutorVars.get_release_from_tag('thermos_R0') == 0
  assert ExecutorVars.get_release_from_tag('thermos_R32') == 32
  assert ExecutorVars.get_release_from_tag('thermos_executor_R12') == 12
  assert ExecutorVars.get_release_from_tag('thermos_smf1-test_16_R32') == 16
  assert ExecutorVars.get_release_from_tag('thermos_executor_smf1-test_23_R10') == 23


def test_extract_pexinfo():
  filename = None
  with temporary_file() as fp:
    filename = fp.name
    with open_zip(filename, 'w') as zf:
      zf.writestr('PEX-INFO', '{"build_properties":{"tag":"thermos_R31337"}}')
    assert ExecutorVars.get_release_from_binary(filename) == 31337
  assert ExecutorVars.get_release_from_binary(filename) == 'UNKNOWN'
  assert ExecutorVars.get_release_from_binary('lololololo') == 'UNKNOWN'


def test_init():
  self = ExecutorVars()
  assert self._orphan == False
  samples = self.metrics.sample()
  assert samples['version'] == 'UNKNOWN'
  assert samples['orphan'] == 0


def test_sample():
  self = ExecutorVars()
  assert self.sample() is True
