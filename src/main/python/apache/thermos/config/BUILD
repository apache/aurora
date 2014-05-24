#
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
  name = 'schema',
  sources = globs('*.py'),
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('3rdparty/python:twitter.common.lang'),
    pants('src/main/python/apache/thermos/common:planner')
  ],
)

python_library(
  name = 'config',
  dependencies = [
    pants(':schema'),
    pants('src/main/python/apache/thermos/common'),  # cover common:planner
  ],
  provides = setup_py(
    name = 'apache.thermos.config',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'Thermos configuration schema and loader.',
  )
)
