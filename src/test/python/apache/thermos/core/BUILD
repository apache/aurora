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
    ':small',
    ':medium',
    ':large',
  ]
)

target(name = 'small',
  dependencies = [
    ':test_process',
  ]
)

target(name = 'medium',
  dependencies = [
    ':test_ephemerals',
    ':test_failing_runner',
    ':test_failure_limit',
    ':test_finalization',
    ':test_helper',
    ':test_runner_integration',
 ]
)

target(name = 'large',
  dependencies = [
    ':test_angry',
    ':test_staged_kill'
  ]
)

python_tests(name = 'test_runner_integration',
  sources = ['test_runner_integration.py'],
  dependencies = [
    'src/main/python/apache/thermos/testing',
  ],
)

python_tests(name = 'test_angry',
  sources = ['test_angry.py'],
  dependencies = [
    'src/main/python/apache/thermos/testing',
  ],
#  timeout = Amount(5, Time.MINUTES)
)

python_tests(name = 'test_ephemerals',
  sources = ['test_ephemerals.py'],
  dependencies = [
    'src/main/python/apache/thermos/testing',
  ],
)

python_tests(name = 'test_failing_runner',
  sources = ['test_failing_runner.py'],
  dependencies = [
    'src/main/python/apache/thermos/testing',
  ],
)

python_tests(name = 'test_finalization',
  sources = ['test_finalization.py'],
  dependencies = [
    'src/main/python/apache/thermos/testing',
  ],
)

python_tests(name = 'test_failure_limit',
  sources = ['test_failure_limit.py'],
  dependencies = [
    'src/main/python/apache/thermos/testing',
  ],
)

python_tests(name = 'test_helper',
  sources = ['test_helper.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:psutil',
    'src/main/python/apache/thermos/core',
  ]
)

python_tests(name = 'test_process',
  sources = ['test_process.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:twitter.common.contextutil',
    'src/main/python/apache/thermos/testing',
  ],
)

python_tests(name = 'test_staged_kill',
  sources = ['test_staged_kill.py'],
  dependencies = [
     '3rdparty/python:twitter.common.process',
     'src/main/python/apache/thermos/testing',
     'src/main/python/apache/thermos/monitoring',
  ]
)
