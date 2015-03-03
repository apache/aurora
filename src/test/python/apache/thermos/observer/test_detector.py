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


import random
from contextlib import contextmanager

import mock
import pytest

from apache.thermos.monitoring.detector import PathDetector
from apache.thermos.observer.detector import ObserverTaskDetector


class EmptyPathDetector(PathDetector):
  def get_paths(self):
    return []


class PatchingObserverTaskDetector(ObserverTaskDetector):
  def __init__(self, *args, **kw):
    self.__tasks = []
    super(PatchingObserverTaskDetector, self).__init__(EmptyPathDetector(), *args, **kw)

  def iter_tasks(self):
    return iter(self.__tasks)

  @contextmanager
  def patched_tasks(self, active_tasks=(), finished_tasks=(), shuffle=True):
    old_tasks = self.__tasks

    tasks = [(root, t_id, 'active') for (root, t_id) in active_tasks] + [
             (root, t_id, 'finished') for (root, t_id) in finished_tasks]

    if shuffle:
      random.shuffle(tasks)

    self.__tasks = tasks
    yield
    self.__tasks = old_tasks


def test_observer_task_detector_construction():
  pod = PatchingObserverTaskDetector()
  pod.refresh()
  assert pod.active_tasks == set()
  assert pod.finished_tasks == set()


# eight transitions:
#    #1 (n/e) -> active
#    #2 (n/e) -> finished
#    #3 active -> active
#    #4 active -> finished
#    #5 active -> (n/e)
#    #6 finished -> active (Fail)
#    #7 finished -> finished
#    #8 finished -> (n/e)
def make_mocks():
  on_active, on_finished, on_removed = mock.Mock(), mock.Mock(), mock.Mock()
  pod = PatchingObserverTaskDetector(
      on_active=on_active,
      on_finished=on_finished,
      on_removed=on_removed,
  )
  return pod, on_active, on_finished, on_removed


TASK1 = ('root1', 'task1')
TASK2 = ('root2', 'task2')


def test_observer_task_detector_standard_transitions():
  pod, on_active, on_finished, on_removed = make_mocks()

  with pod.patched_tasks(active_tasks=(TASK1,)):  # 1
    pod.refresh()
    assert pod.active_tasks == set([TASK1])
    assert pod.finished_tasks == set()
    on_active.assert_called_once_with(*TASK1)
    assert on_finished.call_count == 0
    assert on_removed.call_count == 0
    on_active.reset_mock()

  with pod.patched_tasks(active_tasks=(TASK1,)):  # 3
    pod.refresh()
    assert pod.active_tasks == set([TASK1])
    assert pod.finished_tasks == set()
    assert on_active.call_count == 0
    assert on_finished.call_count == 0
    assert on_removed.call_count == 0

  with pod.patched_tasks(finished_tasks=(TASK1,)):  # 4
    pod.refresh()
    assert pod.active_tasks == set()
    assert pod.finished_tasks == set([TASK1])
    on_finished.assert_called_once_with(*TASK1)
    assert on_active.call_count == 0
    assert on_removed.call_count == 0
    on_finished.reset_mock()

  with pod.patched_tasks(finished_tasks=(TASK1,)):  # 7
    pod.refresh()
    assert pod.active_tasks == set()
    assert pod.finished_tasks == set([TASK1])
    assert on_finished.call_count == 0
    assert on_active.call_count == 0
    assert on_removed.call_count == 0

  with pod.patched_tasks():  # 8
    pod.refresh()
    assert pod.active_tasks == set()
    assert pod.finished_tasks == set()
    on_removed.assert_called_once_with(*TASK1)
    assert on_active.call_count == 0
    assert on_finished.call_count == 0
    on_removed.reset_mock()


def test_observer_task_detector_nonstandard_transitions():
  pod, on_active, on_finished, on_removed = make_mocks()

  with pod.patched_tasks(active_tasks=(TASK1,)):
    pod.refresh()
    assert pod.active_tasks == set([TASK1])
    on_active.reset_mock()

  with pod.patched_tasks():  # 5
    pod.refresh()
    assert pod.active_tasks == set()
    assert pod.finished_tasks == set()
    on_finished.assert_called_once_with(*TASK1)
    on_removed.assert_called_once_with(*TASK1)
    assert on_active.call_count == 0
    on_removed.reset_mock()
    on_finished.reset_mock()

  with pod.patched_tasks(finished_tasks=(TASK2,)):  # 2
    pod.refresh()
    assert pod.active_tasks == set()
    assert pod.finished_tasks == set([TASK2])
    on_active.assert_called_once_with(*TASK2)
    on_finished.assert_called_once_with(*TASK2)
    assert on_removed.call_count == 0
    on_active.reset_mock()
    on_finished.reset_mock()

  with pod.patched_tasks(active_tasks=(TASK2,)):  # 6
    with pytest.raises(AssertionError):
      pod.refresh()
