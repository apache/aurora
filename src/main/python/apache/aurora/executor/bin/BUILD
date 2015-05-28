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
  name = 'thermos_executor_source',
  sources = ['thermos_executor_main.py'],
  dependencies = [
    # To prevent an alpha version of protobuf from being pulled down by pants we
    # specify protobuf here. See AURORA-1128 for more details.
    '3rdparty/python:protobuf',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
    'src/main/python/apache/aurora/executor/common:announcer',
    'src/main/python/apache/aurora/executor/common:executor_detector',
    'src/main/python/apache/aurora/executor/common:executor_timeout',
    'src/main/python/apache/aurora/executor/common:health_checker',
    'src/main/python/apache/aurora/executor/common:resource_manager',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'src/main/python/apache/aurora/executor:executor_vars',
    'src/main/python/apache/aurora/executor:aurora_executor',
    'src/main/python/apache/aurora/executor:thermos_task_runner',
    'src/main/python/apache/thermos/common:path',
  ]
)

python_binary(
  name = 'thermos_executor',
  entry_point = 'apache.aurora.executor.bin.thermos_executor_main:proxy_main',
  always_write_cache = True,
  dependencies = [
    '3rdparty/python:mesos.native',
    ':thermos_executor_source',
  ]
)

python_library(
  name = 'gc_executor_source',
  sources = ['gc_executor_main.py'],
  dependencies = [
    # To prevent an alpha version of protobuf from being pulled down by pants we
    # specify protobuf here. See AURORA-1128 for more details.
    '3rdparty/python:protobuf',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
    'src/main/python/apache/thermos/common:constants',
    'src/main/python/apache/thermos/monitoring:detector',
    'src/main/python/apache/aurora/executor/common:executor_detector',
    'src/main/python/apache/aurora/executor/common:path_detector',
    'src/main/python/apache/aurora/executor:executor_vars',
    'src/main/python/apache/aurora/executor:gc_executor',
  ]
)

python_binary(
  name = 'gc_executor',
  entry_point = 'apache.aurora.executor.bin.gc_executor_main:proxy_main',
  always_write_cache = True,
  dependencies = [
    '3rdparty/python:mesos.native',
    ':gc_executor_source',
  ]
)
