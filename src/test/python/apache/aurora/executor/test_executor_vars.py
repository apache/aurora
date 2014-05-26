#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from twitter.common.contextutil import open_zip, temporary_file

from apache.aurora.executor.executor_vars import ExecutorVars


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
