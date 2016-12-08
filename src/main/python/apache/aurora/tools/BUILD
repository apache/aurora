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
import os


THERMOS_SOURCE = 'thermos.py'
OBSERVER_SOURCE = 'thermos_observer.py'

python_library(
  name = '_tools',
  sources = rglobs('*.py', exclude=[[THERMOS_SOURCE], [OBSERVER_SOURCE]]),
  dependencies = [
    '3rdparty/python:CherryPy',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/executor',
    'src/main/python/apache/thermos/cli',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/monitoring',
    'src/main/python/apache/thermos/observer',
  ],
)

python_library(
  name = '_thermos',
  sources = [THERMOS_SOURCE],
  dependencies = [
    ':_tools',
  ],
)

python_binary(
  name = 'thermos',
  # TODO(ksweeney): Investigate making this use entry_point instead.
  source = THERMOS_SOURCE,
  dependencies = [
    ':_tools',
  ]
)

python_library(
  name = '_thermos_observer',
  sources = [OBSERVER_SOURCE],
  dependencies = [
    ':_tools',
  ],
)

python_binary(
  name = 'thermos_observer',
  # TODO(ksweeney): Investigate making this use entry_point instead.
  source = OBSERVER_SOURCE,
  dependencies = [
    ':_tools',
  ]
)

python_library(
  dependencies = [
    ':_thermos',
    ':_thermos_observer',
    ':_tools',
  ],
  provides = setup_py(
    name = 'apache.aurora.tools',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries({
    'thermos': ':thermos',
    'thermos_observer': ':thermos_observer',
  }),
)
