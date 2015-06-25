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
  name = 'detector',
  sources = ['detector.py'],
  dependencies = [
    '3rdparty/python:twitter.common.lang',
    'src/main/python/apache/thermos/common:constants',
    'src/main/python/apache/thermos/common:path',
  ]
)

python_library(
  name = 'monitor',
  sources = ['monitor.py'],
  dependencies = [
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.recordio',
    'src/main/python/apache/thermos/common:ckpt',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'disk',
  sources = ['disk.py'],
  dependencies = [
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
  ]
)

python_library(
  name = 'process',
  sources = ['process.py'],
)

python_library(
  name = 'process_collector_psutil',
  sources = ['process_collector_psutil.py'],
  dependencies = [
    ':process',
    '3rdparty/python:psutil',
    '3rdparty/python:twitter.common.log',
  ]
)

python_library(
  name = 'resource',
  sources = ['resource.py'],
  dependencies = [
    ':disk',
    ':monitor',
    ':process',
    ':process_collector_psutil',
    '3rdparty/python:twitter.common.collections',
    '3rdparty/python:twitter.common.concurrent',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
  ]
)

python_library(
  name = 'monitoring',
  dependencies = [
    ':detector',
    ':disk',
    ':monitor',
    ':process',
    ':resource',

    # covering dependency for common
    'src/main/python/apache/thermos/common',
  ],
  provides = setup_py(
    name = 'apache.thermos.monitoring',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'Thermos monitoring library.',
  )
)
