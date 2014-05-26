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
    pants(':small'),
    pants(':medium'),
    pants(':large'),
  ]
)

python_test_suite(name = 'small',
  dependencies = [
    pants(':test_process'),
  ]
)

python_test_suite(name = 'medium',
  dependencies = [
    pants(':test_ephemerals'),
    pants(':test_failing_runner'),
    pants(':test_failure_limit'),
    pants(':test_finalization'),
    pants(':test_helper'),
    pants(':test_runner_integration'),
 ]
)

python_test_suite(name = 'large',
  dependencies = [
    pants(':test_angry'),
    pants(':test_staged_kill')
  ]
)

python_tests(name = 'test_runner_integration',
  sources = ['test_runner_integration.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_angry',
  sources = ['test_angry.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
  timeout = Amount(5, Time.MINUTES)
)

python_tests(name = 'test_ephemerals',
  sources = ['test_ephemerals.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_failing_runner',
  sources = ['test_failing_runner.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_finalization',
  sources = ['test_finalization.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_failure_limit',
  sources = ['test_failure_limit.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_helper',
  sources = ['test_helper.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:psutil'),
    pants('src/main/python/apache/thermos/core:helper'),
  ]
)

python_tests(name = 'test_process',
  sources = ['test_process.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:twitter.common.contextutil'),
    pants('src/main/python/apache/thermos/testing:runner'),
  ],
)

python_tests(name = 'test_staged_kill',
  sources = ['test_staged_kill.py'],
  dependencies = [
     pants('3rdparty/python:twitter.common.process'),
     pants('src/main/python/apache/thermos/testing:runner'),
     pants('src/main/python/apache/thermos/monitoring:monitor'),
  ]
)
