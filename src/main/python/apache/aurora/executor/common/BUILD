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
    pants('3rdparty/python:twitter.common.lang'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.metrics'),
  ]
)

python_library(
  name = 'task_runner',
  sources = ['task_runner.py'],
  dependencies = [
    pants(':status_checker'),
    pants('3rdparty/python:twitter.common.lang'),
  ]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    pants(':status_checker'),
    pants(':task_info'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.exceptions'),
    pants('src/main/python/apache/aurora/common:http_signaler'),
  ]
)

python_library(
  name = 'executor_timeout',
  sources = ['executor_timeout.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.exceptions'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
  ]
)

python_library(
  name = 'kill_manager',
  sources = ['kill_manager.py'],
  dependencies = [
    pants(':status_checker'),
  ]
)

python_library(
  name = 'sandbox',
  sources = ['sandbox.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.dirutil'),
    pants('3rdparty/python:twitter.common.log'),
  ]
)

python_library(
  name = 'task_info',
  sources = ['task_info.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/python/apache/aurora/config/schema'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
