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
  name = 'observed_task',
  sources = ['observed_task.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/thermos:pystachio'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/config'),
  ]
)

python_library(
  name = 'task_observer',
  sources = ['task_observer.py'],
  dependencies = [
    pants(':observed_task'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/monitoring:detector'),
    pants('src/main/python/apache/thermos/monitoring:monitor'),
    pants('src/main/python/apache/thermos/monitoring:process'),
    pants('src/main/python/apache/thermos/monitoring:resource'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'observer',
  dependencies = [
    pants(':task_observer'),
    pants('src/main/python/apache/thermos/observer/http:http_observer'),

    # covering libraries
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/python/apache/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'apache.thermos.observer',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'The Thermos observer web interface.',
  ).with_binaries(
    thermos_observer = pants('src/main/python/apache/thermos/observer/bin:thermos_observer'),
  )
)
