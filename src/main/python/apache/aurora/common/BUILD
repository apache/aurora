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

import os

python_library(
  name = 'aurora_job_key',
  sources = ['aurora_job_key.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.lang'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'cluster',
  sources = ['cluster.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
  ]
)

python_library(
  name = 'clusters',
  sources = ['clusters.py'],
  dependencies = [
    pants(':cluster'),
    pants('3rdparty/python:pystachio'),
    pants('3rdparty/python:twitter.common.collections'),
  ]
)

python_library(
  name = 'cluster_option',
  sources = ['cluster_option.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.lang'),
  ]
)

python_library(
  name = 'http_signaler',
  sources = ['http_signaler.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.lang'),
    pants('3rdparty/python:twitter.common.log'),
  ]
)

python_library(
  name = 'common',
  dependencies = [
    pants(':aurora_job_key'),
    pants(':cluster'),
    pants(':cluster_option'),
    pants(':clusters'),
    pants(':http_signaler'),
    pants('src/main/python/apache/aurora/common/auth'),

    # covering dependency on gen.*
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift-packaged'),
  ],
  provides = setup_py(
    name = 'apache.aurora.common',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'Aurora common libraries.',
    license = 'Apache License, Version 2.0',
  )
)
