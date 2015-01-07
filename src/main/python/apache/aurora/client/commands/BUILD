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
  name = 'all',
  dependencies = [
    ':core',
    ':help',
    ':run',
    ':ssh',
  ]
)

python_library(
  name = 'admin',
  sources = ['admin.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/admin:util',
    'src/main/python/apache/aurora/client/api',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/client:config',
    'src/main/python/apache/aurora/client:factory',
    'src/main/python/apache/aurora/common:clusters',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'maintenance',
  sources = ['maintenance.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/admin:host_maintenance',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/common:clusters',
  ]
)

python_library(
  name = 'core',
  sources = ['core.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:pex',
    'src/main/python/apache/aurora/client/api:command_runner',
    'src/main/python/apache/aurora/client/api:disambiguator',
    'src/main/python/apache/aurora/client/api:job_monitor',
    'src/main/python/apache/aurora/client/api:quota_check',
    'src/main/python/apache/aurora/client/api:updater',
    'src/main/python/apache/aurora/client/hooks',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/client:config',
    'src/main/python/apache/aurora/client:factory',
    'src/main/python/apache/aurora/client:options',
    'src/main/python/apache/aurora/common',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'help',
  sources = ['help.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/aurora/client:base',
  ]
)

python_library(
  name = 'run',
  sources = ['run.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/aurora/client/api:command_runner',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/client:options',
    'src/main/python/apache/aurora/common:aurora_job_key',
    'src/main/python/apache/aurora/common:clusters',
  ]
)

python_library(
  name = 'ssh',
  sources = ['ssh.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/aurora/client/api:command_runner',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/client:factory',
    'src/main/python/apache/aurora/client:options',
    'src/main/python/apache/aurora/common:aurora_job_key',
    'src/main/python/apache/aurora/common:clusters',
  ]
)
