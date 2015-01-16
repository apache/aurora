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

python_test_suite(
  name = 'all',
  dependencies = [
    ':announcer',
    ':directory_sandbox',
    ':executor_timeout',
    ':health_checker',
    ':kill_manager',
    ':status_checker',
    ':task_info',
  ]
)

python_library(
  name = 'fixtures',
  sources = ['fixtures.py'],
  dependencies = [
    'src/main/python/apache/aurora/config/schema',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ],
)

python_tests(
  name = 'announcer',
  sources = ['test_announcer.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.testing',
    'src/main/python/apache/aurora/executor/common:announcer',
  ],
)

python_tests(
  name = 'directory_sandbox',
  sources = ['test_directory_sandbox.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/executor/common:sandbox',
  ],
)

python_tests(
  name = 'health_checker',
  sources = ['test_health_checker.py'],
  dependencies = [
    ':fixtures',
    '3rdparty/python:mesos.interface',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.testing',
    'src/main/python/apache/aurora/common:http_signaler',
    'src/main/python/apache/aurora/executor/common:health_checker',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(
  name = 'status_checker',
  sources = ['test_status_checker.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    'src/main/python/apache/aurora/executor/common:status_checker',
  ]
)

python_tests(
  name = 'kill_manager',
  sources = ['test_kill_manager.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    'src/main/python/apache/aurora/executor/common:kill_manager',
    'src/main/python/apache/aurora/executor/common:status_checker',
  ],
)

python_tests(
  name = 'task_info',
  sources = ['test_task_info.py'],
  dependencies = [
    ':fixtures',
    'src/main/python/apache/aurora/executor/common:task_info',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(
  name = 'executor_timeout',
  sources = ['test_executor_timeout.py'],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/executor/common:executor_timeout',
  ],
)

python_tests(
  name = 'resource_manager',
  sources = [ 'test_resource_manager.py' ],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/config:schema',
    'src/main/python/apache/aurora/executor/common:resource_manager',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/monitoring',
  ]
)

python_tests(
  name = 'resource_manager_integration',
  sources = [ 'test_resource_manager_integration.py' ],
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    '3rdparty/python:twitter.common.dirutil',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'src/main/python/apache/aurora/config:schema',
    'src/main/python/apache/aurora/executor/common:sandbox',
    'src/main/python/apache/aurora/executor/common:resource_manager',
  ]
)
