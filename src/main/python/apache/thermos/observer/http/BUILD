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
  name = 'json',
  sources = ['json.py'],
  dependencies = [
    pants('3rdparty/python:twitter.common.http'),
  ]
)

python_library(
  name = 'static_assets',
  sources = ['static_assets.py'],
  resources = rglobs('assets/*'),
  dependencies = [
    pants('3rdparty/python:bottle'),
  ]
)

python_library(
  name = 'templating',
  sources = ['templating.py'],
  resources = globs('templates/*.tpl'),
)

python_library(
  name = 'file_browser',
  sources = ['file_browser.py'],
  dependencies = [
    pants(':templating'),
    pants('3rdparty/python:bottle'),
    pants('3rdparty/python:mako'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.http'),
  ]
)

python_library(
  name = 'http_observer',
  sources = ['__init__.py', 'http_observer.py'],
  dependencies = [
    pants(':file_browser'),
    pants(':json'),
    pants(':static_assets'),
    pants(':templating'),
    pants('3rdparty/python:twitter.common.log'),
    pants('3rdparty/python:twitter.common.http'),
  ]
)
