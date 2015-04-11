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
  name='aurora',
  entry_point = 'apache.aurora.client.cli.client:proxy_main',
  dependencies = [
    ':client_lib'
  ],
)

# TODO(maxim): consider making this default ("aurora") target along with kerberos auth.
python_binary(
  name='kaurora',
  entry_point = 'apache.aurora.client.cli.client:proxy_main',
  dependencies = [
    ':client_lib',
    'src/main/python/apache/aurora/common/auth:kerberos',
  ],
)

python_library(
  name = 'client_lib',
  sources = [ 'client.py' ],
  dependencies = [
    ':cli',
    '3rdparty/python:pex',
  ]
)

python_library(
  name='cli',
  sources = [
    '__init__.py',
    'client.py',
    'config.py',
    'context.py',
    'command_hooks.py',
    'cron.py',
    'jobs.py',
    'options.py',
    'quota.py',
    'sla.py',
    'task.py',
    'update.py',
  ],
  dependencies = [
    '3rdparty/python:pex',
    '3rdparty/python:requests',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/client/api:command_runner',
    'src/main/python/apache/aurora/client/api:job_monitor',
    'src/main/python/apache/aurora/client/api:updater',
    'src/main/python/apache/aurora/client/hooks',
    'src/main/python/apache/aurora/client:base',
    'src/main/python/apache/aurora/client:config',
    'src/main/python/apache/aurora/common',
    'src/main/resources/apache/aurora/client/cli:auroraversion',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ],
)
