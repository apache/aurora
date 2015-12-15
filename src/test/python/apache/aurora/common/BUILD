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
    ':test_aurora_job_key',
    ':test_cluster',
    ':test_clusters',
    ':test_cluster_option',
    ':test_pex_version',
    ':test_shellify',
    ':test_transport',
  ]
)

python_tests(name = 'test_aurora_job_key',
  sources = ['test_aurora_job_key.py'],
  dependencies = [
    'src/main/python/apache/aurora/common'
  ],
)

python_tests(
  name = 'test_pex_version',
  sources = ['test_pex_version.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/common',
  ]
)

python_tests(
  name = 'test_cluster',
  sources = ['test_cluster.py'],
  dependencies = [
    'src/main/python/apache/aurora/common',
  ]
)

python_tests(
  name = 'test_clusters',
  sources = ['test_clusters.py'],
  dependencies = [
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/aurora/common',
  ]
)

python_tests(
  name = 'test_cluster_option',
  sources = ['test_cluster_option.py'],
  dependencies = [
    '3rdparty/python:twitter.common.options',
    'src/main/python/apache/aurora/common',
  ]
)

python_tests(
  name = 'test_shellify',
  sources = ['test_shellify.py'],
  dependencies = [
    'src/main/python/apache/aurora/common',
  ]
)

python_tests(
  name = 'test_transport',
  sources = ['test_transport.py'],
  dependencies = [
    'api/src/main/thrift/org/apache/aurora/gen',
    'src/main/python/apache/aurora/common',
    '3rdparty/python:mock',
  ]
)
