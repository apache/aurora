#
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

import subprocess


def build_and_execute_pex_target(target, binary):
  assert subprocess.call(["./pants", target]) == 0

  # TODO(wickman) Should we extract distdir from pants.ini?
  po = subprocess.Popen([binary, "--help"], stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
  so, se = po.communicate()
  assert po.returncode == 1  # sigh
  assert so.startswith('Options'), 'Unexpected build output: %s' % so


def test_thermos_executor_build():
  build_and_execute_pex_target('src/main/python/apache/aurora/executor/bin:thermos_executor',
                               'dist/thermos_executor.pex')


def test_gc_executor_build():
  build_and_execute_pex_target('src/main/python/apache/aurora/executor/bin:gc_executor',
                               'dist/gc_executor.pex')
