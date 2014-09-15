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
  name = 'thermos_ckpt',
  source = 'thermos_ckpt.py',
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.recordio',
    'src/main/python/apache/thermos/common',
    'src/main/thrift/org/apache/thermos:py-thrift'
  ]
)

python_binary(
  name = 'thermos',
  source = 'thermos.py',
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config:schema',
    'src/main/python/apache/thermos/core',
    'src/main/python/apache/thermos/monitoring',
    'src/main/thrift/org/apache/thermos:py-thrift',
  ],
)
