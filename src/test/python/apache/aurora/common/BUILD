#
# Copyright 2013 Apache Software Foundation
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
    pants(':test_aurora_job_key'),
    pants(':test_cluster'),
    pants(':test_clusters'),
    pants(':test_cluster_option'),
    pants(':test_http_signaler'),
    pants(':test_shellify'),
  ]
)

python_tests(name = 'test_aurora_job_key',
  sources = ['test_aurora_job_key.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/common:aurora_job_key')
  ],
)

python_tests(
  name = 'test_cluster',
  sources = ['test_cluster.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/common:cluster'),
  ]
)

python_tests(
  name = 'test_clusters',
  sources = ['test_clusters.py'],
  dependencies = [
    pants('3rdparty/python:pyyaml'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_tests(
  name = 'test_cluster_option',
  sources = ['test_cluster_option.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.options'),
    pants('src/main/python/apache/aurora/common:cluster'),
    pants('src/main/python/apache/aurora/common:cluster_option'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_tests(
  name = 'test_http_signaler',
  sources = ['test_http_signaler.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/main/python/apache/aurora/common:http_signaler'),
  ]
)

python_tests(
  name = 'test_shellify',
  sources = ['test_shellify.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/common:shellify'),
  ]
)
