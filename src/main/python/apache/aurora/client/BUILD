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
  name = '_client',
  sources = rglobs('*.py'),
  dependencies = [
    '3rdparty/python:certifi',
    '3rdparty/python:chardet',
    '3rdparty/python:idna',
    '3rdparty/python:pex',
    '3rdparty/python:pystachio',
    '3rdparty/python:requests',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.decorators',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.zookeeper',
    '3rdparty/python:urllib3',
    'api/src/main/thrift/org/apache/aurora/gen',
    'src/main/python/apache/aurora/common',
    'src/main/python/apache/aurora/client/cli:auroraversion',
    'src/main/python/apache/aurora/config',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config',
  ],
)

python_binary(
  name='aurora',
  entry_point = 'apache.aurora.client.cli.client:proxy_main',
  dependencies = [
    ':_client'
  ],
)

python_library(
  name = 'client',
  dependencies = [
    ':_client',
  ],
  provides = setup_py(
    name = 'apache.aurora.client',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries({
    'aurora': ':aurora',
  }),
)
