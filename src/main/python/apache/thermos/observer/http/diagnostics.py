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
import sys
import time

from twitter.common.metrics import Label, LambdaGauge

try:
  from pex import PEX
  HAS_PEX = True
except ImportError:
  HAS_PEX = False


def register_diagnostics(metrics):
  rm = metrics.scope('sys')
  now = time.time()
  rm.register(LambdaGauge('uptime', lambda: time.time() - now))
  rm.register(Label('argv', repr(sys.argv)))
  rm.register(Label('path', repr(sys.path)))
  rm.register(Label('version', sys.version))
  rm.register(Label('platform', sys.platform))
  rm.register(Label('executable', sys.executable))
  rm.register(Label('prefix', sys.prefix))
  rm.register(Label('exec_prefix', sys.exec_prefix))
  rm.register(Label('uname', ' '.join(os.uname())))


def register_build_properties(metrics):
  if not HAS_PEX:
    return
  rm = metrics.scope('build')
  try:
    build_properties = PEX().info.build_properties
  except PEX.NotFound:
    return
  for key, value in build_properties.items():
    rm.register(Label(str(key), str(value)))
