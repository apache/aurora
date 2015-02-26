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
from collections import namedtuple

from twitter.common.dirutil import safe_bsize, safe_delete, safe_mtime, safe_rmtree
from twitter.common.quantity import Amount, Data, Time

from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.common.path import TaskPath

from .detector import TaskDetector


class TaskGarbageCollector(object):
  """A task wrapper to manage its sandbox and log state."""

  def __init__(self, checkpoint_root, task_id):
    """
    :param checkpoint_root: The checkpoint root to find the given task.
    :param task_id: The task_id of the task whose state we wish to manage.
    """

    self._detector = TaskDetector(checkpoint_root)
    self._task_id = task_id
    self._pathspec = TaskPath(root=checkpoint_root, task_id=task_id)
    self._state = CheckpointDispatcher.from_file(self._detector.get_checkpoint(task_id))

  def get_age(self):
    return safe_mtime(self._detector.get_checkpoint(self._task_id))

  def get_metadata(self, with_size=True):
    runner_ckpt = self._detector.get_checkpoint(self._task_id)
    process_ckpts = [ckpt for ckpt in self._detector.get_process_checkpoints(self._task_id)]
    # assumes task is in finished state.
    json_spec = self._pathspec.given(state='finished').getpath('task_path')
    for path in [json_spec, runner_ckpt] + process_ckpts:
      if with_size:
        yield path, safe_bsize(path)
      else:
        yield path

  def get_logs(self, with_size=True):
    if self._state and self._state.header and self._state.header.log_dir:
      for path in self._detector.get_process_logs(self._task_id, self._state.header.log_dir):
        if with_size:
          yield path, safe_bsize(path)
        else:
          yield path

  def get_data(self, with_size=True):
    if self._state and self._state.header and self._state.header.sandbox:
      for root, dirs, files in os.walk(self._state.header.sandbox):
        for file in files:
          filename = os.path.join(root, file)
          if with_size:
            yield filename, safe_bsize(filename)
          else:
            yield filename

  def erase_task(self):
    self.erase_data()
    self.erase_logs()
    self.erase_metadata()

  def erase_metadata(self):
    for fn in self.get_metadata(with_size=False):
      safe_delete(fn)
    safe_rmtree(self._pathspec.getpath('checkpoint_path'))

  def erase_logs(self):
    for fn in self.get_logs(with_size=False):
      safe_delete(fn)
    if self._state and self._state.header:
      path = self._pathspec.given(log_dir=self._state.header.log_dir).getpath('process_logbase')
      safe_rmtree(path)

  def erase_data(self):
    for fn in self.get_data(with_size=False):
      safe_delete(fn)
    if self._state and self._state.header and self._state.header.sandbox:
      safe_rmtree(self._state.header.sandbox)


class GarbageCollectionPolicy(object):
  def __init__(self,
               path_detector,
               max_age=Amount(10 ** 10, Time.DAYS),
               max_space=Amount(10 ** 10, Data.TB),
               max_tasks=10 ** 10,
               include_metadata=True,
               include_logs=True,
               verbose=False,
               logger=sys.stdout.write):
    """
      Default garbage collection policy.

      Arguments that may be specified:
        max_age:   Amount(Time) (max age of a retained task)  [default: infinity]
        max_space: Amount(Data) (max space to keep)           [default: infinity]
        max_tasks: int (max number of tasks to keep)          [default: infinity]
        include_metadata: boolean  (Whether or not to include metadata in the
          space calculations.)  [default: True]
        include_logs: boolean  (Whether or not to include logs in the
          space calculations.)  [default: True]
        verbose: boolean (whether or not to log)  [default: False]
        logger: callable (function to call with log messages) [default: sys.stdout.write]
    """
    self._path_detector = path_detector
    self._max_age = max_age
    self._max_space = max_space
    self._max_tasks = max_tasks
    self._include_metadata = include_metadata
    self._include_logs = include_logs
    self._verbose = verbose
    self._logger = logger

  def log(self, msg):
    if self._verbose:
      self._logger(msg)

  def get_finished_tasks(self):
    """Yields (checkpoint_root, task_id) for finished tasks."""

    for checkpoint_root in self._path_detector.get_paths():
      for task_id in TaskDetector(checkpoint_root).get_task_ids(state='finished'):
        yield (checkpoint_root, task_id)

  def run(self):
    tasks = []
    now = time.time()

    # age: The time (in seconds) since the last task transition to/from ACTIVE/FINISHED
    # metadata_size: The size of the thermos checkpoint records for this task
    # log_size: The size of the stdout/stderr logs for this task's processes
    # data_size: The size of the sandbox of this task.
    TaskTuple = namedtuple('TaskTuple',
        'checkpoint_root task_id age metadata_size log_size data_size')

    for checkpoint_root, task_id in self.get_finished_tasks():
      collector = TaskGarbageCollector(checkpoint_root, task_id)

      age = Amount(int(now - collector.get_age()), Time.SECONDS)
      self.log('Analyzing task %s (age: %s)... ' % (task_id, age))
      metadata_size = Amount(sum(sz for _, sz in collector.get_metadata()), Data.BYTES)
      self.log('  metadata %.1fKB ' % metadata_size.as_(Data.KB))
      log_size = Amount(sum(sz for _, sz in collector.get_logs()), Data.BYTES)
      self.log('  logs %.1fKB ' % log_size.as_(Data.KB))
      data_size = Amount(sum(sz for _, sz in collector.get_data()), Data.BYTES)
      self.log('  data %.1fMB ' % data_size.as_(Data.MB))
      tasks.append(TaskTuple(checkpoint_root, task_id, age, metadata_size, log_size, data_size))

    gc_tasks = set()
    gc_tasks.update(task for task in tasks if task.age > self._max_age)

    self.log('After age filter: %s tasks' % len(gc_tasks))

    def total_gc_size(task):
      return sum([task.data_size,
                  task.metadata_size if self._include_metadata else Amount(0, Data.BYTES),
                  task.log_size if self._include_logs else Amount(0, Data.BYTES)],
                  Amount(0, Data.BYTES))

    total_used = Amount(0, Data.BYTES)
    for task in sorted(tasks, key=lambda tsk: tsk.age, reverse=True):
      if task not in gc_tasks:
        total_used += total_gc_size(task)
        if total_used > self._max_space:
          gc_tasks.add(task)
    self.log('After size filter: %s tasks' % len(gc_tasks))

    for task in sorted(tasks, key=lambda tsk: tsk.age, reverse=True):
      if task not in gc_tasks and len(tasks) - len(gc_tasks) > self._max_tasks:
        gc_tasks.add(task)
    self.log('After total task filter: %s tasks' % len(gc_tasks))

    self.log('Deciding to garbage collect the following tasks:')
    if gc_tasks:
      for task in gc_tasks:
        self.log('   %s' % repr(task))
    else:
      self.log('   None.')

    return gc_tasks
