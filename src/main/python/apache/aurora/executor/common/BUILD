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
  name = 'status_checker',
  sources = ['status_checker.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
  ]
)

python_library(
  name = 'task_runner',
  sources = ['task_runner.py'],
  dependencies = [
    ':status_checker',
    '3rdparty/python:twitter.common.lang',
  ]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    ':status_checker',
    ':task_info',
    '3rdparty/python:mesos.interface',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.exceptions',
    'src/main/python/apache/aurora/common:http_signaler',
  ]
)

python_library(
  name = 'announcer',
  sources = ['announcer.py'],
  dependencies = [
    '3rdparty/python:twitter.common.concurrent',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.zookeeper',
    ':status_checker',
    ':task_info',
  ]
)

python_library(
  name = 'executor_timeout',
  sources = ['executor_timeout.py'],
  dependencies = [
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
  ]
)

python_library(
  name = 'kill_manager',
  sources = ['kill_manager.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    ':status_checker',
  ]
)

python_library(
  name = 'sandbox',
  sources = ['sandbox.py'],
  dependencies = [
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.log',
  ]
)

python_library(
  name = 'task_info',
  sources = ['task_info.py'],
  dependencies = [
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/config',
    'src/main/python/apache/aurora/config/schema',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)
