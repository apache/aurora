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
    pants('3rdparty/python:psutil'),
    pants('3rdparty/python:twitter.common.dirutil'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('3rdparty/python:twitter.common.recordio'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'inspector',
  sources = ['inspector.py'],
  dependencies = [
    pants(':muxer'),
    pants('3rdparty/python:twitter.common.recordio'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'muxer',
  sources = ['muxer.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.recordio'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'process',
  sources = ['process.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.dirutil'),
    pants('3rdparty/python:twitter.common.lang'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('3rdparty/python:twitter.common.recordio'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'runner',
  sources = ['__init__.py', 'runner.py'],
  dependencies = [
    pants(':helper'),
    pants(':muxer'),
    pants(':process'),
    pants('3rdparty/python:psutil'),
    pants('3rdparty/python:twitter.common.dirutil'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('3rdparty/python:twitter.common.recordio'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/common:planner'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'core',
  dependencies = [
    pants(':inspector'),
    pants(':runner'),

    # covering libs
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ],
  provides = setup_py(
    name = 'apache.thermos.core',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'The Thermos core state machine.',
  )
)
