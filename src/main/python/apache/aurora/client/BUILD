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

# Create an alias for the previous target
python_library(
  name = 'api',
  dependencies = [
    pants('src/main/python/apache/aurora/client/api')
  ]
)

python_library(
  name = 'base',
  sources = ['base.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'binding_helper',
  sources = ['binding_helper.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.lang'),
  ]
)

python_library(
  name = 'config',
  sources = ['config.py'],
  dependencies = [
    pants(':base'),
    pants(':binding_helper'),
    pants('3rdparty/python:pystachio'),
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'factory',
  sources = ['factory.py'],
  dependencies = [
    pants(':base'),
    pants('3rdparty/python:twitter.common.app'),
    pants('src/main/python/apache/aurora/client/hooks'),
    pants('src/main/python/apache/aurora/common:cluster'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_library(
  name = 'options',
  sources = ['options.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/common:options'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
  ]
)

python_library(
  name = 'client-packaged',
  dependencies = [
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/python/apache/aurora/config:config-packaged'),
    pants('src/main/python/apache/thermos/common'),
  ],
  provides = setup_py(
    name = 'apache.aurora.client',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    aurora_admin = pants('src/main/python/apache/aurora/client/bin:aurora_admin'),
    aurora = pants('src/main/python/apache/aurora/client/bin:aurora_client'),
  )
)

python_library(
  name = 'clientv2-packaged',
  dependencies = [
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/python/apache/thermos/common'),
  ],
  provides = setup_py(
    name = 'apache.aurora.clientv2',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    aurora2 = pants('src/main/python/apache/aurora/client/cli:aurora2'),
  )
)

