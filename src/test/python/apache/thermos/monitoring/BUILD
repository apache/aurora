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
    ':test_disk',
    ':test_detector',
    ':test_resource',
  ]
)

python_tests(name = 'test_disk',
  sources = ['test_disk.py'],
  dependencies = [
    'src/main/python/apache/thermos/monitoring',
    '3rdparty/python:twitter.common.quantity',
  ]
)

python_tests(name = 'test_detector',
  sources = ['test_detector.py'],
  dependencies = [
    '3rdparty/python:twitter.common.contextutil',
    '3rdparty/python:twitter.common.dirutil',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/monitoring',
  ]
)

python_tests(name = 'test_resource',
  sources = ['test_resource.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/thermos/common',
    'src/main/python/apache/thermos/monitoring',
  ]
)
