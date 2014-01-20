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

python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor_main.py',
  entry_point = 'apache.aurora.executor.bin.thermos_executor_main:proxy_main',
  ignore_errors = True,
  always_write_cache = True,
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('src/main/python/apache/aurora/executor/common:executor_timeout'),
    pants('src/main/python/apache/aurora/executor/common:health_checker'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora/executor:executor_detector'),
    pants('src/main/python/apache/aurora/executor:executor_vars'),
    pants('src/main/python/apache/aurora/executor:thermos_executor'),
    pants('src/main/python/apache/aurora/executor:thermos_task_runner'),
  ]
)

python_binary(
  name = 'gc_executor',
  source = 'gc_executor_main.py',
  entry_point = 'apache.aurora.executor.bin.gc_executor_main:proxy_main',
  ignore_errors = True,
  always_write_cache = True,
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/aurora/executor:executor_detector'),
    pants('src/main/python/apache/aurora/executor:executor_vars'),
    pants('src/main/python/apache/aurora/executor:gc_executor'),
  ]
)

python_binary(
  name = 'thermos_runner',
  source = 'thermos_runner_main.py',
  entry_point = 'apache.aurora.executor.bin.thermos_runner_main:proxy_main',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/aurora/executor:thermos_runner'),
  ],
)
