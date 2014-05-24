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
from abc import abstractproperty

from pystachio import Environment
from twitter.common import log
from twitter.common.lang import AbstractClass

from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.config.loader import ThermosTaskWrapper
from apache.thermos.config.schema import ThermosContext


class ObservedTask(AbstractClass):
  """ Represents a Task being observed """

  @classmethod
  def safe_mtime(cls, path):
    try:
      return os.path.getmtime(path)
    except OSError:
      return None

  def __init__(self, task_id, pathspec):
    self._task_id = task_id
    self._pathspec = pathspec
    self._mtime = self._get_mtime()

  @abstractproperty
  def type(self):
    """Indicates the type of task (active or finished)"""

  def _read_task(self, memoized={}):
    """Read the corresponding task from disk and return a ThermosTask.  Memoizes already-read tasks.
    """
    if self._task_id not in memoized:
      path = self._pathspec.given(task_id=self._task_id, state=self.type).getpath('task_path')
      if os.path.exists(path):
        task = ThermosTaskWrapper.from_file(path)
        if task is None:
          log.error('Error reading ThermosTask from %s in observer.' % path)
        else:
          context = self.context(self._task_id)
          if not context:
            log.warning('Task not yet available: %s' % self._task_id)
          task = task.task() % Environment(thermos=context)
          memoized[self._task_id] = task

    return memoized.get(self._task_id, None)

  def _get_mtime(self):
    """Retrieve the mtime of the task's state directory"""
    get_path = lambda state: self._pathspec.given(
      task_id=self._task_id, state=state).getpath('task_path')
    mtime = self.safe_mtime(get_path('active'))
    if mtime is None:
      mtime = self.safe_mtime(get_path('finished'))
    if mtime is None:
      log.error("Couldn't get mtime for task %s!" % self._task_id)
    return mtime

  def context(self, task_id):
    state = self.state
    if state.header is None:
      return None
    return ThermosContext(
      ports=state.header.ports if state.header.ports else {},
      task_id=state.header.task_id,
      user=state.header.user,
    )

  @property
  def task(self):
    """Return a ThermosTask representing this task"""
    return self._read_task()

  @property
  def task_id(self):
    """Return the task's task_id"""
    return self._task_id

  @property
  def mtime(self):
    """Return mtime of task file"""
    return self._mtime

  @abstractproperty
  def state(self):
    """Return state of task (gen.apache.thermos.ttypes.RunnerState)"""


class ActiveObservedTask(ObservedTask):
  """An active Task known by the TaskObserver"""

  def __init__(self, task_id, pathspec, task_monitor, resource_monitor):
    super(ActiveObservedTask, self).__init__(task_id, pathspec)
    self._task_monitor = task_monitor
    self._resource_monitor = resource_monitor

  @property
  def type(self):
    return 'active'

  @property
  def state(self):
    """Return a RunnerState representing the current state of task, retrieved from TaskMonitor"""
    return self.task_monitor.get_state()

  @property
  def task_monitor(self):
    """Return a TaskMonitor monitoring this task"""
    return self._task_monitor

  @property
  def resource_monitor(self):
    """Return a ResourceMonitor implementation monitoring this task's resources"""
    return self._resource_monitor


class FinishedObservedTask(ObservedTask):
  """A finished Task known by the TaskObserver"""

  def __init__(self, task_id, pathspec):
    super(FinishedObservedTask, self).__init__(task_id, pathspec)
    self._state = None

  @property
  def type(self):
    return 'finished'

  @property
  def state(self):
    """Return final state of Task (RunnerState, read from disk and cached for future access)"""
    if self._state is None:
      path = self._pathspec.given(task_id=self._task_id).getpath('runner_checkpoint')
      self._state = CheckpointDispatcher.from_file(path)
    return self._state
