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
  name = 'api',
  sources = ['__init__.py'],
  dependencies = [
    ':restarter',
    ':scheduler_client',
    ':sla',
    ':updater',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/common',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'command_runner',
  sources = ['command_runner.py'],
  dependencies = [
    ':api',
    '3rdparty/python:pystachio',
    'src/main/python/apache/thermos/config:schema',
    'src/main/python/apache/aurora/common:cluster',
    'src/main/python/apache/aurora/config:schema',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'job_monitor',
  sources = ['job_monitor.py'],
  dependencies = [
    ':task_util',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'restarter',
  sources = ['restarter.py'],
  dependencies = [
    ':instance_watcher',
    ':updater_util',
    '3rdparty/python:twitter.common.log',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'scheduler_client',
  sources = ['scheduler_client.py'],
  dependencies = [
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.rpc',
    '3rdparty/python:twitter.common.zookeeper',
    'src/main/python/apache/aurora/common/auth',
    'src/main/python/apache/aurora/common:cluster',
    'src/main/python/apache/aurora/common:transport',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'error_handling_thread',
  sources = ['error_handling_thread.py'],
  dependencies = [
    '3rdparty/python:twitter.common.decorators',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.log',
  ]
)

python_library(
  name = 'scheduler_mux',
  sources = ['scheduler_mux.py'],
  dependencies = [
    ':error_handling_thread',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.log',
  ]
)

python_library(
  name = 'task_util',
  sources = ['task_util.py'],
  dependencies = [
    ':scheduler_mux',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'instance_watcher',
  sources = ['instance_watcher.py', 'health_check.py'],
  dependencies = [
    ':scheduler_client',
    ':scheduler_mux',
    ':task_util',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/common:http_signaler',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'quota_check',
  sources = ['quota_check.py'],
  dependencies = [
    ':scheduler_client',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'sla',
  sources = ['sla.py'],
  dependencies = [
    ':scheduler_client',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/common',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'updater',
  sources = ['updater.py'],
  dependencies = [
    ':scheduler_client',
    ':instance_watcher',
    ':job_monitor',
    ':updater_util',
    ':quota_check',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'updater_util',
  sources = ['updater_util.py'],
  dependencies = [
    '3rdparty/python:twitter.common.log',
  ]
)
