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
    pants(':admin'),
    pants(':core'),
    pants(':hooks'),
    pants(':maintenance'),
    pants(':run'),
    pants(':ssh'),
  ])

python_tests(
  name='core',
  sources = [
    'test_cancel_update.py',
    'test_create.py',
    'test_diff.py',
    'test_kill.py',
    'test_listjobs.py',
    'test_restart.py',
    'test_status.py',
    'test_update.py'
  ],
  dependencies = [
    pants(':util'),
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/client/commands:core'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ])

python_tests(
  name='hooks',
  sources = [ 'test_hooks.py' ],
  dependencies = [
    pants(':util'),
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/client/commands:core'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ])

python_tests(
  name='admin',
  sources = [
    'test_admin_sla.py',
  ],
  dependencies = [
    pants(':util'),
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/client/commands:admin'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ])

python_tests(
  name='ssh',
  sources = [
    'test_ssh.py',
  ],
  dependencies = [
    pants(':util'),
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/client/commands:ssh'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ])

python_tests(
  name='run',
  sources = [
    'test_run.py',
  ],
  dependencies = [
    pants(':util'),
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/client/commands:run'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ])

python_tests(
  name='maintenance',
  sources = [
    'test_maintenance.py',
  ],
  dependencies = [
    pants(':util'),
    pants('3rdparty/python:mock'),
    pants('src/main/python/apache/aurora/client/commands:maintenance'),
  ])

python_library(
  name='util',
  sources = [ 'util.py' ],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/main/python/apache/aurora/client/commands:core'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
