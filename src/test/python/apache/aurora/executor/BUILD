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

python_test_suite(name = 'all',
  dependencies = [
    ':executor-small',
    ':executor-large',
  ]
)


python_test_suite(
  name = 'executor-small',
  dependencies = [
    ':executor_base',
    ':executor_vars',
    ':status_manager',
    ':thermos_task_runner',
    'src/test/python/apache/aurora/executor/common:all',
  ],
)


python_test_suite(
  name = 'executor-large',
  dependencies = [
    ':gc_executor',
    ':thermos_executor',
  ]
)


python_tests(name = 'gc_executor',
  sources = ['test_gc_executor.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.concurrent',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.testing',
    'src/main/python/apache/thermos/common:path',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/core:runner',
    'api/src/main/thrift/org/apache/thermos:py-thrift',
    'src/main/python/apache/aurora/executor:gc_executor',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ],
)

python_tests(name = 'thermos_executor',
  sources = ['test_thermos_executor.py'],
#  timeout = Amount(5, Time.MINUTES),
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/core',
    'src/main/python/apache/thermos/monitoring:monitor',
    'src/main/python/apache/aurora/config:schema',
    'src/main/python/apache/aurora/executor/common:executor_timeout',
    'src/main/python/apache/aurora/executor/common:health_checker',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'src/main/python/apache/aurora/executor/common:task_runner',
    'src/main/python/apache/aurora/executor:aurora_executor',
    'src/main/python/apache/aurora/executor:thermos_task_runner',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(name = 'thermos_task_runner',
  sources = ['test_thermos_task_runner.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:twitter.common.contextutil',
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/config:schema',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'src/main/python/apache/aurora/executor:thermos_task_runner',
  ]
)

python_tests(name = 'executor_base',
  sources = ['test_executor_base.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/executor:executor_base',
  ],
)

python_tests(name = 'status_manager',
  sources = ['test_status_manager.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/executor:status_manager',
    'src/main/python/apache/aurora/executor/common:status_checker',
  ],
)

python_tests(name = 'executor_vars',
  sources = ['test_executor_vars.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/executor:executor_vars',
  ]
)
