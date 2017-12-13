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


resources(
  name = 'ui',
  sources = zglobs('**/assets/*', '**/*.tpl'),
)

python_library(
  name = 'observer',
  sources = rglobs('*.py'),
  dependencies = [
    '3rdparty/python:bottle',
    '3rdparty/python:mako',
    '3rdparty/python:pex',
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.http',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
    '3rdparty/python:twitter.common.quantity',
    'api/src/main/thrift/org/apache/thermos',

    # covering libraries
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/monitoring',

    ':ui',
  ],
  provides = setup_py(
    name = 'apache.thermos.observer',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'The Thermos observer web interface libraries.',
  ),
)
