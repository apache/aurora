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

target(
  name = 'all',
  dependencies = [
    ':common',
    ':main',
    'src/test/python/apache/thermos/cli/commands',
  ]
)

python_tests(
  name = 'common',
  sources = ['test_common.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/thermos/cli',
  ]
)

python_tests(
  name = 'main',
  sources = ['test_main.py'],
  dependencies = [
    'src/main/python/apache/thermos/cli',
  ]
)
