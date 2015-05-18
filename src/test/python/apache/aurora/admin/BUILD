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

python_test_suite(name = 'all',
  dependencies = [
    ':admin',
    ':host_maintenance',
    ':maintenance'
  ],
)

python_tests(
  name='admin',
  sources = [
    'test_admin.py',
    'test_admin_sla.py',
    'test_admin_util.py',
  ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/admin:admin',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ],
)

python_tests(name = 'host_maintenance',
  sources = ['test_host_maintenance.py'],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.quantity',
    'src/main/python/apache/aurora/client:api',
    'src/main/python/apache/aurora/common:cluster',
    'src/main/python/apache/aurora/admin:host_maintenance',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ],
)

python_tests(
  name='maintenance',
  sources = [
    'test_maintenance.py',
  ],
  dependencies = [
    ':util',
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/admin:maintenance',
  ],
)

python_library(
  name='util',
  sources = [ 'util.py' ],
  dependencies = [
    '3rdparty/python:mock',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'src/main/python/apache/aurora/client/hooks:hooks',
    'src/main/python/apache/aurora/client:config',
    'src/test/python/apache/aurora:api_util',
  ],
)
