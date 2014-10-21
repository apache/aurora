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

python_test_suite(
  name = 'all',
  dependencies = [
    ':api',
    ':bridge',
    ':command_hooks',
    ':cron',
    ':help',
    ':inspect',
    ':job',
    ':config',
    ':logging',
    ':plugins',
    ':quota',
    ':sla',
    ':task',
    ':supdate',
    ':update'
  ]
)

python_library(
  name = 'util',
  sources = [ 'util.py' ],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/client/cli',
  ]
)

python_tests(
  name='inspect',
  sources = [ 'test_inspect.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
  ]
)

python_tests(
  name='supdate',
  sources = [ 'test_supdate.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name = 'help',
  sources = [ 'test_help.py' ],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
  ]
)

python_tests(
  name = 'command_hooks',
  sources = [ 'test_command_hooks.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name = 'logging',
  sources = [ 'test_logging.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
  ]
)

python_tests(
  name = 'bridge',
  sources = [ 'test_bridge.py' ],
  dependencies = [
    'src/main/python/apache/aurora/client/cli:bridge',
  ]
)

python_tests(
  name = 'api',
  sources = [ 'test_api_from_cli.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)


python_tests(
  name = 'config',
  sources = [ 'test_config_noun.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name='update',
  sources=['test_update.py'],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name = 'job',
  sources = [
    'test_cancel_update.py',
    'test_create.py',
    'test_diff.py',
    'test_kill.py',
    'test_open.py',
    'test_restart.py',
    'test_status.py',
  ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name = 'cron',
  sources = [
    'test_cron.py',
  ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)


python_tests(
  name = 'quota',
  sources = [ 'test_quota.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name = 'sla',
  sources = [ 'test_sla.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name='task',
  sources = [ 'test_task_run.py'],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)

python_tests(
  name = 'plugins',
  sources = [ 'test_plugins.py' ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/client/cli',
    'src/main/python/apache/aurora/client/cli:client_lib',
    'src/test/python/apache/aurora/client/commands:util'
  ]
)
