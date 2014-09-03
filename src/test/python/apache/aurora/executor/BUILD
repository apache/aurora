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
    pants(':executor-small'),
    pants(':executor-large'),
  ]
)


python_test_suite(
  name = 'executor-small',
  dependencies = [
    pants(':executor_base'),
    pants(':executor_detector'),
    pants(':executor_vars'),
    pants(':status_manager'),
    pants(':thermos_task_runner'),
    pants('src/test/python/apache/aurora/executor/common:all'),
  ],
)


python_test_suite(
  name = 'executor-large',
  dependencies = [
    # TODO(jfarrell): AURORA-14 for executor_builds
    #pants(':executor_builds'),
    pants(':gc_executor'),
    pants(':thermos_executor'),
  ]
)


python_tests(name = 'gc_executor',
  sources = ['test_gc_executor.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.concurrent'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('3rdparty/python:twitter.common.testing'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/python/apache/thermos/core:runner'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
    pants('src/main/python/apache/aurora/executor:gc_executor'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ],
)

python_tests(name = 'executor_detector',
  sources = [ 'test_executor_detector.py' ],
  dependencies = [
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('3rdparty/python:twitter.common.dirutil'),
    pants('src/main/python/apache/aurora/executor:executor_detector')
  ]
)

python_tests(name = 'thermos_executor',
  sources = ['test_thermos_executor.py'],
  timeout = Amount(5, Time.MINUTES),
  dependencies = [
    pants('3rdparty/python:twitter.common.app'),
    pants('3rdparty/python:twitter.common.exceptions'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos/monitoring:monitor'),
    pants('src/main/python/apache/aurora/config:schema'),
    pants('src/main/python/apache/aurora/executor/common:executor_timeout'),
    pants('src/main/python/apache/aurora/executor/common:health_checker'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora/executor/common:task_runner'),
    pants('src/main/python/apache/aurora/executor:aurora_executor'),
    pants('src/main/python/apache/aurora/executor:thermos_task_runner'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'thermos_task_runner',
  sources = ['test_thermos_task_runner.py'],
  dependencies = [
    pants('3rdparty/python:mesos.interface'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('3rdparty/python:twitter.common.dirutil'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('src/main/python/apache/aurora/config:schema'),
    pants('src/main/python/apache/aurora/executor/common:sandbox'),
    pants('src/main/python/apache/aurora/executor:thermos_task_runner'),
  ]
)

python_tests(name = 'executor_base',
  sources = ['test_executor_base.py'],
  dependencies = [
    pants('3rdparty/python:mesos.interface'),
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/executor:executor_base'),
  ],
)

python_tests(name = 'status_manager',
  sources = ['test_status_manager.py'],
  dependencies = [
    pants('3rdparty/python:mesos.interface'),
    pants('3rdparty/python:mock'),
    pants('src/main/python/apache/aurora/executor:status_manager'),
    pants('src/main/python/apache/aurora/executor/common:status_checker'),
  ],
)

python_tests(name = 'executor_vars',
  sources = ['test_executor_vars.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/aurora/executor:executor_vars'),
  ]
)

python_tests(name = 'executor_builds',
  sources = ['test_executor_builds.py'],
)
