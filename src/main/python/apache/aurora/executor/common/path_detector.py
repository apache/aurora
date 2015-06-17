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
    def iterate():
      for scan_result in self._detector:
        yield os.path.join(os.path.realpath(ExecutorDetector.path(scan_result)), self._sandbox_path)
    return list(set(path for path in iterate() if os.path.exists(path)))
