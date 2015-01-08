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
    'src/main/python/apache/aurora/client/api'
  ]
)

python_library(
  name = 'base',
  sources = ['base.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'src/main/python/apache/aurora/common',
  ]
)

python_library(
  name = 'binding_helper',
  sources = ['binding_helper.py'],
  dependencies = [
    '3rdparty/python:twitter.common.lang',
  ]
)

python_library(
  name = 'config',
  sources = ['config.py'],
  dependencies = [
    ':base',
    ':binding_helper',
    '3rdparty/python:pystachio',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/config',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'factory',
  sources = ['factory.py'],
  dependencies = [
    ':base',
    '3rdparty/python:twitter.common.app',
    'src/main/python/apache/aurora/client/hooks',
    'src/main/python/apache/aurora/common:cluster',
    'src/main/python/apache/aurora/common:clusters',
  ]
)

python_library(
  name = 'client-packaged',
  dependencies = [
    'src/main/python/apache/aurora/common',
    'src/main/python/apache/aurora/config:config-packaged',
    'src/main/python/apache/thermos/common',
  ],
  provides = setup_py(
    name = 'apache.aurora.client',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    aurora = 'src/main/python/apache/aurora/client/cli:aurora',
  )
)

