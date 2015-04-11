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
  name = 'admin',
  sources = ['admin.py', 'admin_util.py'],
  dependencies = [
    ':help',
    ':util',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/client/api',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/common',
    'src/main/python/apache/aurora/client:config',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'help',
  sources = ['help.py'],
  dependencies = [
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base'
  ]
)

python_library(
  name = 'host_maintenance',
  sources = ['host_maintenance.py'],
  dependencies = [
    ':util',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/client:api',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_library(
  name = 'maintenance',
  sources = ['maintenance.py'],
  dependencies = [
    ':util',
    ':host_maintenance',
    '3rdparty/python:twitter.common.app',
    '3rdparty/python:twitter.common.log',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/common:clusters',
  ]
)

python_library(
  name = 'util',
  sources = ['admin_util.py'],
  dependencies = [
    'src/main/python/apache/aurora/client:base',
  ]
)

python_binary(
  name = 'aurora_admin',
  entry_point = 'apache.aurora.admin.aurora_admin:proxy_main',
  dependencies = [
    ':aurora_admin_lib'
  ]
)

# TODO(maxim): consider making this default ("aurora_admin") target along with kerberos auth.
python_binary(
  name = 'kaurora_admin',
  entry_point = 'apache.aurora.admin.aurora_admin:proxy_main',
  dependencies = [
    ':aurora_admin_lib',
    'src/main/python/apache/aurora/common/auth:kerberos',
  ],
)

python_library(
  name = 'aurora_admin_lib',
  sources = [ 'aurora_admin.py' ],
  dependencies = [
      ':admin',
      ':maintenance',
    ]
)

python_library(
  name = 'admin-packaged',
  dependencies = [
    ':aurora_admin_lib',
    'src/main/python/apache/aurora/config:config-packaged',
  ],
  provides = setup_py(
    name = 'apache.aurora.admin',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    aurora_admin = ':aurora_admin',
  )
)
