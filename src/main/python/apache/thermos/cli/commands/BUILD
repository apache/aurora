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

python_library(
  name = 'commands',
  dependencies = [
    ':help',
    ':inspect',
    ':kill',
    ':read',
    ':run',
    ':simplerun',
    ':status',
    ':tail',
  ]
)

python_library(
  name = 'help',
  sources = ['help.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
  ]
)

python_library(
  name = 'inspect',
  sources = ['inspect.py'],
  dependencies = [
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/thermos/cli:common',
    'src/main/python/apache/thermos/common:options',
  ]
)

python_library(
  name = 'kill',
  sources = ['kill.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/thermos/cli:common',
    'src/main/python/apache/thermos/core:helper',
  ]
)

python_library(
  name = 'read',
  sources = ['read.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.recordio',
    'src/main/python/apache/thermos/common:ckpt',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'run',
  sources = ['run.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/thermos/cli:common',
    'src/main/python/apache/thermos/common:options',
  ]
)

python_library(
  name = 'simplerun',
  sources = ['simplerun.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/thermos/cli:common',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/config:schema',
  ]
)

python_library(
  name = 'status',
  sources = ['status.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/thermos/common:ckpt',
    'src/main/python/apache/thermos/monitoring:detector',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'tail',
  sources = ['tail.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.dirutil',
    'src/main/python/apache/thermos/common:ckpt',
    'src/main/python/apache/thermos/common:path',
    'src/main/python/apache/thermos/monitoring:detector',
    'src/main/python/apache/thermos/monitoring:monitor',
  ]
)

