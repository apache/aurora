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
  name='aurora2',
  entry_point = 'apache.aurora.client.cli.client:proxy_main',
  dependencies = [
    pants(':client_lib')
  ],
)

python_library(
  name = 'bridge',
  sources = ['bridge.py']
)

python_library(
  name = 'client_lib',
  sources = [ 'client.py' ],
  dependencies = [
    pants(':cli'),
    pants(':bridge'),
    pants('3rdparty/python:twitter.common.python'),
    pants('src/main/python/apache/aurora/client/bin:aurora_client_lib')
  ]
)

python_library(
  name='cli',
  sources = [
    '__init__.py',
    'config.py',
    'context.py',
    'command_hooks.py',
    'jobs.py',
    'options.py',
    'quota.py',
    'sla.py',
    'task.py'
  ],
  dependencies = [
    pants('3rdparty/python:argparse'),
    pants('3rdparty/python:requests'),
    pants('3rdparty/python:twitter.common.python'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('src/main/python/apache/aurora/client/api:command_runner'),
    pants('src/main/python/apache/aurora/client/api:disambiguator'),
    pants('src/main/python/apache/aurora/client/api:job_monitor'),
    pants('src/main/python/apache/aurora/client/api:updater'),
    pants('src/main/python/apache/aurora/client/hooks'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/client:config'),
    pants('src/main/python/apache/aurora/client:factory'),
    pants('src/main/python/apache/aurora/client:options'),
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ],
)
