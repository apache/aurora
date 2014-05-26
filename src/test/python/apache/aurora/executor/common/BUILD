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
    pants(':directory_sandbox'),
    pants(':health_checker'),
    pants(':status_checker'),
    pants(':task_info'),
  ]
)

python_tests(
  name = 'directory_sandbox',
  sources = ['test_directory_sandbox.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
  ],
)

python_tests(
  name = 'health_checker',
  sources = ['test_health_checker.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('3rdparty/python:twitter.common.testing'),
    pants('src/main/python/apache/aurora/executor/common:health_checker'),
  ]
)

python_tests(
  name = 'status_checker',
  sources = ['test_status_checker.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/executor/common:status_checker'),
  ]
)

python_tests(
  name = 'task_info',
  sources = ['test_task_info.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/executor/common:task_info'),
    pants('src/main/python/apache/aurora/config/schema'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
