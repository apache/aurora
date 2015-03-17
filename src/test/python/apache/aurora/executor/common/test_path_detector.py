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

import mock

from apache.aurora.executor.common.executor_detector import ExecutorDetector
from apache.aurora.executor.common.path_detector import MesosPathDetector


class Match(object):
  def __init__(self, root, slave, framework, executor, run):
    self._groups = dict(
        root=root, slave_id=slave, framework_id=framework, executor_id=executor, run=run)

  def groups(self):
    return self._groups


def test_path_detector():
  ROOTS = ('/var/lib/mesos1/slaves', '/var/lib/mesos2/slaves')
  FAKE_ROOT = '/var/blah/blah'
  FAKE_CHECKPOINT_DIR = 'ckpt'

  path1, path2 = (
      ExecutorDetector.path(Match(ROOTS[0], 'slave001', 'framework1', 'executor1', 'latest')),
      ExecutorDetector.path(Match(ROOTS[1], 'slave002', 'framework2', 'executor2', 'latest')),
  )

  with mock.patch('glob.glob', return_value=(path1, path2)) as glob:
    with mock.patch('os.path.exists', side_effect=(True, False)) as exists:
      mpd = MesosPathDetector(root=FAKE_ROOT, sandbox_path=FAKE_CHECKPOINT_DIR)
      paths = list(mpd.get_paths())
      assert len(paths) == 1
      assert paths == [os.path.join(path1, FAKE_CHECKPOINT_DIR)]

      expected_glob_pattern = os.path.join(*ExecutorDetector.PATTERN) % {
        'root': FAKE_ROOT,
        'slave_id': '*',
        'framework_id': '*',
        'executor_id': '*',
        'run': '*'
      }
      glob.assert_called_once_with(expected_glob_pattern)
      assert exists.mock_calls == [
          mock.call(os.path.join(path1, FAKE_CHECKPOINT_DIR)),
          mock.call(os.path.join(path2, FAKE_CHECKPOINT_DIR)),
      ]
