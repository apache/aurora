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

python_library(
  name = 'observed_task',
  sources = ['observed_task.py'],
  dependencies = [
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/thermos/common:ckpt',
    'src/main/python/apache/thermos/config',
  ]
)

python_library(
  name = 'task_observer',
  sources = ['task_observer.py'],
  dependencies = [
    ':observed_task',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/thermos/common:path',
    'src/main/python/apache/thermos/monitoring:detector',
    'src/main/python/apache/thermos/monitoring:monitor',
    'src/main/python/apache/thermos/monitoring:process',
    'src/main/python/apache/thermos/monitoring:resource',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'observer',
  dependencies = [
    ':task_observer',
    'src/main/python/apache/thermos/observer/http:http_observer',

    # covering libraries
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/monitoring',
  ],
  provides = setup_py(
    name = 'apache.thermos.observer',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'The Thermos observer web interface.',
  ).with_binaries(
    thermos_observer = 'src/main/python/apache/thermos/observer/bin:thermos_observer',
  )
)
