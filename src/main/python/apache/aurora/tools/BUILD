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

python_binary(
  name = 'thermos',
  source = 'thermos.py',
  dependencies = [
    'src/main/python/apache/thermos/cli:common',
    'src/main/python/apache/thermos/cli:main',
    'src/main/python/apache/aurora/executor/common:path_detector',
  ]
)

python_binary(
  name = 'thermos_observer',
  source = 'thermos_observer.py',
  dependencies = [
    '3rdparty/python:CherryPy',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/executor/common:path_detector',
    'src/main/python/apache/thermos/common:constants',
    'src/main/python/apache/thermos/monitoring:detector',
    'src/main/python/apache/thermos/observer:task_observer',
    'src/main/python/apache/thermos/observer/http:configure',
  ]
)
