#
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
    pants(':binding_helper'),
    pants(':config'),
    pants('src/test/python/apache/aurora/client/api:all'),
    pants('src/test/python/apache/aurora/client/cli:all'),
    pants('src/test/python/apache/aurora/client/commands:all'),
    pants('src/test/python/apache/aurora/client/hooks:all'),
  ],
)

python_tests(
  name = 'binding_helper',
  sources = ['test_binding_helper.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/client:binding_helper'),
    pants('src/main/python/apache/aurora/config'),
  ],
)

python_tests(name = 'config',
  sources = ['test_config.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/main/python/apache/aurora/client:config'),
  ],
)

python_library(
  name = 'fake_scheduler_proxy',
  sources = ['fake_scheduler_proxy.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/client/api:scheduler_client'),
  ]
)
