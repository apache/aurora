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
    pants(':core'),
    pants(':help'),
    pants(':run'),
    pants(':ssh'),
  ]
)

python_library(
  name = 'admin',
  sources = ['admin.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('src/main/python/apache/aurora/client/api'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/common:clusters'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'maintenance',
  sources = ['maintenance.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/admin:host_maintenance'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_library(
  name = 'core',
  sources = ['core.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.python'),
    pants('src/main/python/apache/aurora/client/api:command_runner'),
    pants('src/main/python/apache/aurora/client/api:disambiguator'),
    pants('src/main/python/apache/aurora/client/api:job_monitor'),
    pants('src/main/python/apache/aurora/client/api:quota_check'),
    pants('src/main/python/apache/aurora/client/api:updater'),
    pants('src/main/python/apache/aurora/client/hooks'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/client:config'),
    pants('src/main/python/apache/aurora/client:factory'),
    pants('src/main/python/apache/aurora/client:options'),
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'help',
  sources = ['help.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('src/main/python/apache/aurora/client:base'),
  ]
)

python_library(
  name = 'run',
  sources = ['run.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('src/main/python/apache/aurora/client/api:command_runner'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/client:options'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_library(
  name = 'ssh',
  sources = ['ssh.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('src/main/python/apache/aurora/client/api:command_runner'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/client:factory'),
    pants('src/main/python/apache/aurora/client:options'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)
