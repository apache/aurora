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
  name = 'aurora_client',
  entry_point = 'apache.aurora.client.bin.aurora_client:proxy_main',
  dependencies = [
    ':aurora_client_lib'
  ]
)

python_library(
  name = 'aurora_client_lib',
  sources = [ 'aurora_client.py' ],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client/commands:all',
    'src/main/python/apache/aurora/client:base',
  ]
)

python_binary(
  name = 'aurora_admin',
  entry_point = 'apache.aurora.client.bin.aurora_admin:proxy_main',
  dependencies = [
    ':aurora_admin_lib'
  ]
)

python_library(
  name = 'aurora_admin_lib',
  sources = [ 'aurora_admin.py' ],
  dependencies = [
      '3rdparty/python:twitter.common.app',
      '3rdparty/python:twitter.common.log',
      'src/main/python/apache/aurora/client/commands:admin',
      'src/main/python/apache/aurora/client/commands:help',
      'src/main/python/apache/aurora/client/commands:maintenance',
      'src/main/python/apache/aurora/client:base',
      'src/main/python/apache/aurora/client:options',
    ]
)
