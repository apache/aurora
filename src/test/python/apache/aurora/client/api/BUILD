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
    pants(':disambiguator'),
    pants(':job_monitor'),
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':instance_watcher'),
    pants(':updater'),
    pants(':quota_check'),
    pants(':sla'),
  ],
)

python_tests(
  name = 'disambiguator',
  sources = ['test_disambiguator.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('3rdparty/python:twitter.common.app'),
    pants('src/main/python/apache/aurora/client/api:disambiguator'),
  ],
)

python_tests(name = 'job_monitor',
  sources = ['test_job_monitor.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/main/python/apache/aurora/client/api:api'),
    pants('src/main/python/apache/aurora/client/api:job_monitor'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
    pants('src/test/python/apache/aurora/client:fake_scheduler_proxy'),
  ],
)

python_tests(name = 'restarter',
  sources = ['test_restarter.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/main/python/apache/aurora/client/api:restarter'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
    pants('src/test/python/apache/aurora/client:fake_scheduler_proxy'),
  ],
)

python_tests(name = 'scheduler_client',
  sources = ['test_scheduler_client.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('3rdparty/python:mox'),
    pants('src/main/python/apache/aurora/client/api:scheduler_client'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'instance_watcher',
  sources = ['test_instance_watcher.py', 'test_health_check.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/main/python/apache/aurora/client/api:instance_watcher'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'quota_check',
  sources = ['test_quota_check.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/main/python/apache/aurora/client/api:quota_check'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'sla',
  sources = ['test_sla.py'],
  dependencies = [
    pants('3rdparty/python:mock'),
    pants('src/main/python/apache/aurora/client/api:sla'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_tests(name = 'updater',
  sources = ['test_updater.py'],
  dependencies = [
    pants('3rdparty/python:mox'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
    pants('src/main/python/apache/aurora/client/api:updater'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
    pants('src/test/python/apache/aurora/client:fake_scheduler_proxy'),
  ]
)
