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
  name = 'thermos_task_runner',
  sources = ['thermos_task_runner.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config:schema',
    'src/main/python/apache/thermos/core',
    'src/main/python/apache/thermos/monitoring:monitor',
    'src/main/python/apache/aurora/common:http_signaler',
    'src/main/python/apache/aurora/executor/common:status_checker',
    'src/main/python/apache/aurora/executor/common:task_info',
    'src/main/python/apache/aurora/executor/common:task_runner',
  ]
)


python_library(
  name = 'executor_vars',
  sources = ['executor_vars.py'],
  dependencies = [
    '3rdparty/python:pex',
    '3rdparty/python:psutil',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.metrics',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.string',
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/executor/common:status_checker',
  ]
)

python_library(
  name = 'executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:twitter.common.log',
  ]
)

python_library(
  name = 'aurora_executor',
  sources = ['aurora_executor.py'],
  dependencies = [
    ':status_manager',
    ':executor_base',
    '3rdparty/python:mesos.interface',
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.concurrent',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/executor/common:kill_manager',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'src/main/python/apache/aurora/executor/common:task_info',
    'src/main/python/apache/aurora/executor/common:task_runner',
  ]
)


python_library(
  name = 'gc_executor',
  sources = ['gc_executor.py'],
  dependencies = [
    ':executor_base',
    '3rdparty/python:mesos.interface',
    '3rdparty/python:psutil',
    '3rdparty/python:twitter.common.collections',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/thermos/common:ckpt',
    'src/main/python/apache/thermos/common:path',
    'src/main/python/apache/thermos/core:helper',
    'src/main/python/apache/thermos/core:inspector',
    'src/main/python/apache/thermos/monitoring:detector',
    'src/main/python/apache/thermos/monitoring:garbage',
    'src/main/python/apache/aurora/config:schema',
    'src/main/python/apache/aurora/executor/common:executor_detector',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
  ]
)

python_library(
  name = 'executor-packaged',
  dependencies = [
    # Covering dependencies
    'src/main/python/apache/aurora/common',
    'src/main/python/apache/aurora/config',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/core',
    'src/main/python/apache/thermos/monitoring',
  ],
  provides = setup_py(
    name = 'apache.aurora.executor',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    gc_executor = 'src/main/python/apache/aurora/executor/bin:gc_executor',
    thermos_executor = 'src/main/python/apache/aurora/executor/bin:thermos_executor',
  )
)
