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

import pytest

from apache.thermos.common.path import TaskPath


def test_legacy_task_roots():
  assert TaskPath(root='/var/lib/mesos').given(task_id='foo').getpath('checkpoint_path').startswith(
      '/var/lib/mesos')


def test_legacy_log_dirs():
  assert TaskPath(log_dir='sloth_love_chunk').given(task_id='foo').getpath(
      'process_logbase') == 'sloth_love_chunk'


def test_exception_on_none_keys():
  with pytest.raises(ValueError):
    TaskPath(root=None).given(task_id='foo').getpath('checkpoint_path')
  with pytest.raises(ValueError):
    TaskPath(root='/var/lib/mesos').given(task_id=None).getpath('checkpoint_path')
