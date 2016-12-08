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
  name = '_executor',
  sources = rglobs('*.py'),
  dependencies = [
    '3rdparty/python:mesos.interface',
    '3rdparty/python:pex',
    # To prevent an alpha version of protobuf from being pulled down by pants we
    # specify protobuf here. See AURORA-1128 for more details.
    '3rdparty/python:protobuf',
    '3rdparty/python:psutil',
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.concurrent',
    '3rdparty/python:twitter.common.dirutil',
    '3rdparty/python:twitter.common.exceptions',
    '3rdparty/python:twitter.common.lang',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.metrics',
    '3rdparty/python:twitter.common.quantity',
    '3rdparty/python:twitter.common.string',
    '3rdparty/python:twitter.common.zookeeper',
    'api/src/main/thrift/org/apache/aurora/gen',
    'src/main/python/apache/aurora/common',
    'src/main/python/apache/aurora/config',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/config',
    'src/main/python/apache/thermos/core',
    'src/main/python/apache/thermos/monitoring',
  ],
)

python_binary(
  name = 'thermos_executor',
  entry_point = 'apache.aurora.executor.bin.thermos_executor_main:proxy_main',
  always_write_cache = True,
  dependencies = [
    ':_executor',
    # TODO(ksweeney): This violates the rule that every library export the same deps as its
    # contained binaries.
    '3rdparty/python:mesos.executor',
  ]
)

python_library(
  dependencies = [
    ':_executor',
  ],
  provides = setup_py(
    name = 'apache.aurora.executor',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    thermos_executor = ':thermos_executor',
  )
)
