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
    '3rdparty/python:twitter.common.http',
  ]
)

python_library(
  name = 'static_assets',
  sources = ['static_assets.py'],
  resources = rglobs('assets/*'),
  dependencies = [
    '3rdparty/python:bottle',
  ]
)

python_library(
  name = 'templating',
  sources = ['templating.py'],
  resources = globs('templates/*.tpl'),
)

python_library(
  name = 'diagnostics',
  sources = ['diagnostics.py'],
  dependencies = [
    '3rdparty/python:pex',
    '3rdparty/python:twitter.common.metrics',
  ]
)

python_library(
  name = 'vars_endpoint',
  sources = ['vars_endpoint.py'],
  dependencies = [
    '3rdparty/python:twitter.common.http',
    '3rdparty/python:twitter.common.metrics',
  ]
)

python_library(
  name = 'configure',
  sources = ['configure.py'],
  dependencies = [
    '3rdparty/python:twitter.common.http',
    ':diagnostics',
    ':http_observer',
    ':vars_endpoint',
  ]
)

python_library(
  name = 'file_browser',
  sources = ['file_browser.py'],
  dependencies = [
    ':templating',
    '3rdparty/python:bottle',
    '3rdparty/python:mako',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.http',
  ]
)

python_library(
  name = 'http_observer',
  sources = ['__init__.py', 'http_observer.py'],
  dependencies = [
    ':file_browser',
    ':json',
    ':static_assets',
    ':templating',
    '3rdparty/python:twitter.common.log',
    '3rdparty/python:twitter.common.http',
  ]
)
