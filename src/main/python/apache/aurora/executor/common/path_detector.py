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

from apache.thermos.monitoring.detector import PathDetector

from .executor_detector import ExecutorDetector


class MesosPathDetector(PathDetector):
  DEFAULT_MESOS_ROOT = '/var/lib/mesos'
  DEFAULT_SANDBOX_PATH = 'checkpoints'

  def __init__(self, root=DEFAULT_MESOS_ROOT, sandbox_path=DEFAULT_SANDBOX_PATH):
    self._detector = ExecutorDetector(root)
    self._sandbox_path = sandbox_path

  def get_paths(self):
    result = []

    for scan_result in self._detector:
      executor_path = ExecutorDetector.path(scan_result)
      sandbox_path = os.path.join(executor_path, self._sandbox_path)

      # We only care about the realpath of executors and not the additional `latest` link
      if not os.path.islink(executor_path) and os.path.exists(sandbox_path):
        result.append(sandbox_path)

    return result
