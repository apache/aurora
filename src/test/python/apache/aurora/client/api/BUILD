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
    ':api',
    ':instance_watcher',
    ':job_monitor',
    ':mux',
    ':quota_check',
    ':restarter',
    ':scheduler_client',
    ':sla',
    ':task_util',
    ':updater',
    ':updater_util'
  ],
)

python_tests(name = 'api',
  sources = ['test_api.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/client/api:api',
    'src/main/python/apache/aurora/client:config',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift'
  ],
)

python_tests(name = 'job_monitor',
  sources = ['test_job_monitor.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/client/api:api',
    'src/main/python/apache/aurora/client/api:job_monitor',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'src/test/python/apache/aurora/client:fake_scheduler_proxy',
  ],
)

python_tests(name = 'instance_watcher',
  sources = ['test_instance_watcher.py', 'test_health_check.py'],
  dependencies = [
    '3rdparty/python:mox',
    'src/main/python/apache/aurora/client/api:instance_watcher',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(name = 'mux',
  sources = ['test_scheduler_mux.py'],
  dependencies = [
    'src/main/python/apache/aurora/client/api:scheduler_mux',
  ]
)

python_tests(name = 'quota_check',
  sources = ['test_quota_check.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/client/api:quota_check',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(name = 'restarter',
  sources = ['test_restarter.py'],
  dependencies = [
    '3rdparty/python:mox',
    'src/main/python/apache/aurora/client/api:restarter',
    'src/main/python/apache/aurora/common:aurora_job_key',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'src/test/python/apache/aurora/client:fake_scheduler_proxy',
  ],
)

python_tests(name = 'scheduler_client',
  sources = ['test_scheduler_client.py'],
  dependencies = [
    '3rdparty/python:mock',
    '3rdparty/python:mox',
    'src/main/python/apache/aurora/client/api:scheduler_client',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(name = 'sla',
  sources = ['test_sla.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/client/api:sla',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(name = 'task_util',
  sources = ['test_task_util.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/main/python/apache/aurora/client/api:scheduler_client',
    'src/main/python/apache/aurora/client/api:task_util',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
  ]
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    '3rdparty/python:mox',
    'src/main/python/apache/aurora/common:aurora_job_key',
    'src/main/python/apache/aurora/client/api:updater',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift',
    'src/test/python/apache/aurora/client:fake_scheduler_proxy',
  ]
)

python_tests(name = 'updater_util',
  sources = ['test_updater_util.py'],
  dependencies = [
    'src/main/python/apache/aurora/client/api:api',
    'api/src/main/thrift/org/apache/aurora/gen:py-thrift'
  ],
)
