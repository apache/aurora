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

python_library(
  name = 'api',
  sources = ['__init__.py'],
  dependencies = [
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':sla'),
    pants(':updater'),
    pants('3rdparty/python:twitter.common.lang'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'command_runner',
  sources = ['command_runner.py'],
  dependencies = [
    pants(':api'),
    pants('3rdparty/python:pystachio'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/python/apache/aurora/common:cluster'),
    pants('src/main/python/apache/aurora/config:schema'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'disambiguator',
  sources = ['disambiguator.py'],
  dependencies = [
    pants(':api'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/common'),
  ]
)

python_library(
  name = 'job_monitor',
  sources = ['job_monitor.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.quantity'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'restarter',
  sources = ['restarter.py'],
  dependencies = [
    pants(':instance_watcher'),
    pants(':updater_util'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'scheduler_client',
  sources = ['scheduler_client.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.quantity'),
    pants('3rdparty/python:twitter.common.rpc'),
    pants('3rdparty/python:twitter.common.zookeeper'),
    pants('src/main/python/apache/aurora/common/auth'),
    pants('src/main/python/apache/aurora/common:cluster'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'instance_watcher',
  sources = ['instance_watcher.py', 'health_check.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants('3rdparty/python:twitter.common.lang'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/common:http_signaler'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'quota_check',
  sources = ['quota_check.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'sla',
  sources = ['sla.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'updater',
  sources = ['updater.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants(':instance_watcher'),
    pants(':updater_util'),
    pants(':quota_check'),
    pants('3rdparty/python:twitter.common.log'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'updater_util',
  sources = ['updater_util.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.log'),
  ]
)
