from __future__ import print_function

from abc import abstractmethod, ABCMeta
from collections import namedtuple
import os
import sys
import time

from twitter.common.dirutil import safe_delete, safe_rmtree, safe_bsize
from twitter.common.quantity import Amount, Data, Time
from twitter.thermos.base.ckpt import CheckpointDispatcher
from twitter.thermos.base.path import TaskPath
from twitter.thermos.monitoring.detector import TaskDetector


class TaskGarbageCollector(object):
  def __init__(self, root):
    self._root = root
    self._detector = TaskDetector(root=self._root)
    self._states = {}

  @classmethod
  def _log(cls, msg):
    print(msg, file=sys.stderr)

  def state(self, task_id):
    if task_id not in self._states:
      self._states[task_id] = CheckpointDispatcher.from_file(self._detector.get_checkpoint(task_id))
    return self._states[task_id]

  def get_age(self, task_id):
    return os.path.getmtime(self._detector.get_checkpoint(task_id))

  def get_finished_tasks(self):
    return [task_id for _, task_id in self._detector.get_task_ids(state='finished')]

  def get_metadata(self, task_id, with_size=True):
    runner_ckpt = self._detector.get_checkpoint(task_id)
    process_ckpts = [ckpt for ckpt in self._detector.get_process_checkpoints(task_id)]
    json_spec = TaskPath(root=self._root, task_id=task_id, state='finished').getpath('task_path')
    for path in [json_spec, runner_ckpt] + process_ckpts:
      if with_size:
        yield path, safe_bsize(path)
      else:
        yield path

  def get_logs(self, task_id, with_size=True):
    state = self.state(task_id)
    if state and state.header:
      for path in self._detector.get_process_logs(task_id, state.header.log_dir):
        if with_size:
          yield path, safe_bsize(path)
        else:
          yield path

  def get_data(self, task_id, with_size=True):
    state = self.state(task_id)
    if state and state.header and state.header.sandbox:
      for root, dirs, files in os.walk(state.header.sandbox):
        for file in files:
          filename = os.path.join(root, file)
          if with_size:
            yield filename, safe_bsize(filename)
          else:
            yield filename

  def erase_task(self, task_id):
    self.erase_data(task_id)
    self.erase_logs(task_id)
    self.erase_metadata(task_id)

  def erase_metadata(self, task_id):
    for fn in self.get_metadata(task_id, with_size=False):
      safe_delete(fn)
    safe_rmtree(TaskPath(root=self._root, task_id=task_id).getpath('checkpoint_path'))

  def erase_logs(self, task_id):
    for fn in self.get_logs(task_id, with_size=False):
      safe_delete(fn)
    state = self.state(task_id)
    if state and state.header:
      safe_rmtree(TaskPath(root=self._root, task_id=task_id, log_dir=state.header.log_dir)
                  .getpath('process_logbase'))

  def erase_data(self, task_id):
    # TODO(wickman)
    # This could be potentially dangerous if somebody naively runs their sandboxes in e.g.
    # $HOME or / or similar.  Perhaps put a guard somewhere?
    for fn in self.get_data(task_id, with_size=False):
      os.unlink(fn)
    state = self.state(task_id)
    if state and state.header and state.header.sandbox:
      safe_rmtree(state.header.sandbox)


class TaskGarbageCollectionPolicy(object):
  __metaclass__ = ABCMeta

  def __init__(self, collector):
    assert isinstance(collector, TaskGarbageCollector)
    self._collector = collector

  @property
  def collector(self):
    return self._collector

  @abstractmethod
  def run(self):
    """
      Returns a list of task_ids that should be garbage collected given the specified policy.
    """
    pass


class DefaultCollector(TaskGarbageCollectionPolicy):
  def __init__(self, collector, **kw):
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
    self._max_age = kw.get('max_age', Amount(10**10, Time.DAYS))
    self._max_space = kw.get('max_space', Amount(10**10, Data.TB))
    self._max_tasks = kw.get('max_tasks', 10**10)
    self._include_metadata = kw.get('include_metadata', True)
    self._include_logs = kw.get('include_logs', True)
    self._verbose = kw.get('verbose', False)
    self._logger = kw.get('logger', sys.stdout.write)
    TaskGarbageCollectionPolicy.__init__(self, collector)

  def log(self, msg):
    if self._verbose:
      self._logger(msg)

  def run(self):
    tasks = []
    now = time.time()

    TaskTuple = namedtuple('TaskTuple', 'task_id age metadata_size log_size data_size')
    for task_id in self.collector.get_finished_tasks():
      age = Amount(int(now - self.collector.get_age(task_id)), Time.SECONDS)
      self.log('Analyzing task %s (age: %s)... ' % (task_id, age))
      metadata_size = Amount(sum(sz for _, sz in self.collector.get_metadata(task_id)), Data.BYTES)
      self.log('  metadata %.1fKB ' % metadata_size.as_(Data.KB))
      log_size = Amount(sum(sz for _, sz in self.collector.get_logs(task_id)), Data.BYTES)
      self.log('  logs %.1fKB ' % log_size.as_(Data.KB))
      data_size = Amount(sum(sz for _, sz in self.collector.get_data(task_id)), Data.BYTES)
      self.log('  data %.1fMB ' % data_size.as_(Data.MB))
      tasks.append(TaskTuple(task_id, age, metadata_size, log_size, data_size))

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
