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
  name = 'helper',
  sources = ['helper.py'],
  dependencies = [
    '3rdparty/python:psutil',
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.recordio',
    'src/main/python/apache/thermos/common:ckpt',
    'src/main/python/apache/thermos/common:path',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'muxer',
  sources = ['muxer.py'],
  dependencies = [
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.recordio',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'process',
  sources = ['process.py'],
  dependencies = [
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.recordio',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'runner',
  sources = ['__init__.py', 'runner.py'],
  dependencies = [
    ':helper',
    ':muxer',
    ':process',
    '3rdparty/python:psutil',
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.recordio',
    'src/main/python/apache/thermos/common:ckpt',
    'src/main/python/apache/thermos/common:path',
    'src/main/python/apache/thermos/common:planner',
    'src/main/python/apache/thermos/config:schema',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'core',
  dependencies = [
    ':runner',

    # covering libs
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ],
  provides = setup_py(
    name = 'apache.thermos.core',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'The Thermos core state machine.',
  )
)
