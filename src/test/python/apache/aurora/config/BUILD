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

target(name = 'all',
  dependencies = [
    ':test_base',
    ':test_constraint_parsing',
    ':test_loader',
    ':test_thrift',
  ]
)

python_tests(name = 'test_base',
  sources = ['test_base.py'],
  dependencies = [
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/config',
  ]
)

python_tests(name = 'test_loader',
  sources = ['test_loader.py'],
  dependencies = [
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/config',
  ],
  coverage = [
    'apache.aurora.config',
    'apache.thermos.config'
  ]
)

python_tests(name = 'test_constraint_parsing',
  sources = ['test_constraint_parsing.py'],
  dependencies = [
    'src/main/python/apache/aurora/config',
  ]
)

python_tests(name = 'test_thrift',
  sources = ['test_thrift.py'],
  dependencies = [
    'src/main/python/apache/aurora/config',
    'api/src/main/thrift/org/apache/aurora/gen',
  ]
)
