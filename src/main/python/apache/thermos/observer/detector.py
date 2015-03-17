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


from collections import namedtuple

from apache.thermos.monitoring.detector import PathDetector, TaskDetector

RootedTask = namedtuple('RootedTask', ('root task_id'))


class ObserverTaskDetector(object):
  """ObserverTaskDetector turns on-disk thermos task transitions into callback events."""

  @classmethod
  def maybe_callback(cls, callback):
    if callback is not None:
      return callback
    return lambda: True

  def __init__(self,
               path_detector,
               on_active=None,
               on_finished=None,
               on_removed=None):

    if not isinstance(path_detector, PathDetector):
      raise TypeError('ObserverTaskDetector takes PathDetector, got %s' % type(path_detector))

    self._path_detector = path_detector
    self._active_tasks = set()  # (root, task_id) tuple
    self._finished_tasks = set()  # (root, task_id) tuple
    self._detectors = {}
    self._on_active = self.maybe_callback(on_active)
    self._on_finished = self.maybe_callback(on_finished)
    self._on_removed = self.maybe_callback(on_removed)

  @property
  def active_tasks(self):
    return self._active_tasks.copy()

  @property
  def finished_tasks(self):
    return self._finished_tasks.copy()

  def _refresh_detectors(self):
    new_paths = set(self._path_detector.get_paths())
    old_paths = set(self._detectors)

    for path in old_paths - new_paths:
      self._detectors.pop(path)

    for path in new_paths - old_paths:
      self._detectors[path] = TaskDetector(root=path)

  def iter_tasks(self):
    # returns an iterator of root, task_id, active/finished
    for root, detector in self._detectors.items():
      for status, task_id in detector.get_task_ids():
        yield (root, task_id, status)

  def refresh(self):
    self._refresh_detectors()

    all_active, all_finished = set(), set()

    for root, task_id, status in self.iter_tasks():
      task = RootedTask(root, task_id)

      if status == 'active':
        all_active.add(task)
        if task in self._active_tasks:
          continue
        elif task in self._finished_tasks:
          assert False, 'Unexpected state.'
        else:
          self._active_tasks.add(task)
          self._on_active(root, task_id)
      elif status == 'finished':
        all_finished.add(task)
        if task in self._active_tasks:
          self._active_tasks.remove(task)
          self._finished_tasks.add(task)
          self._on_finished(root, task_id)
        elif task in self._finished_tasks:
          continue
        else:
          self._finished_tasks.add(task)
          self._on_active(root, task_id)
          self._on_finished(root, task_id)
      else:
        assert False, 'Unknown state.'

    all_tasks = all_active | all_finished

    for task in self.active_tasks - all_tasks:
      self._on_finished(task.root, task.task_id)
      self._on_removed(task.root, task.task_id)

    for task in self.finished_tasks - all_tasks:
      self._on_removed(task.root, task.task_id)

    self._active_tasks = all_active
    self._finished_tasks = all_finished
