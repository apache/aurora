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
  name = 'thermos_task_runner',
  sources = ['thermos_task_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos/monitoring:monitor'),
    pants('src/main/python/apache/aurora/common:http_signaler'),
    pants('src/main/python/apache/aurora/executor/common:status_checker'),
    pants('src/main/python/apache/aurora/executor/common:task_info'),
    pants('src/main/python/apache/aurora/executor/common:task_runner'),
  ]
)

python_library(
  name = 'executor_detector',
  sources = ['executor_detector.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'executor_vars',
  sources = ['executor_vars.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:psutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('aurora/twitterdeps/src/python/twitter/common/python'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/aurora/executor/common:status_checker'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
    pants('src/main/python/apache/aurora:mesos-core'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:pystachio'),
    pants(':status_manager'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/concurrent'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/aurora/executor/common:kill_manager'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora/executor/common:task_info'),
    pants('src/main/python/apache/aurora/executor/common:task_runner'),
    pants('src/main/python/apache/aurora:mesos-core'),
  ]
)

python_library(
  name = 'thermos_runner',
  sources = ['thermos_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/thermos/common:planner'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/python/apache/thermos/core'),
  ],
)

python_library(
  name = 'gc_executor',
  sources = ['gc_executor.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:psutil'),
    pants(':executor_detector'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/common/collections'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/core:helper'),
    pants('src/main/python/apache/thermos/core:inspector'),
    pants('src/main/python/apache/thermos/monitoring:detector'),
    pants('src/main/python/apache/thermos/monitoring:garbage'),
    pants('src/main/python/apache/aurora/config:schema'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora:mesos-core'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'executor-packaged',
  dependencies = [
    # Covering dependencies
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'apache.aurora.executor',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    gc_executor = pants('src/main/python/apache/aurora/executor/bin:gc_executor'),
    thermos_executor = pants('src/main/python/apache/aurora/executor/bin:thermos_executor'),
    thermos_runner = pants('src/main/python/apache/aurora/executor/bin:thermos_runner'),
  )
)
