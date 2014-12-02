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

page(name = 'readme', source = 'README.md')

python_library(
  name = 'thermos',
  dependencies = [
    'src/main/python/apache/thermos/core',
    'src/main/python/apache/thermos/monitoring',
  ],
  provides = setup_py(
    name = 'apache.thermos',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    descripton = 'The Apache Aurora Thermos runtime.',
  ).with_binaries(
    thermos = 'src/main/python/apache/thermos/bin:thermos',
    thermos_runner = 'src/main/python/apache/thermos/bin:thermos_runner',
  )
)
