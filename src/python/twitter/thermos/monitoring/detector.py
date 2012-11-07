import glob
import os
import re

from twitter.thermos.base.path import TaskPath


class TaskDetector(object):
  """
    Helper class in front of TaskPath to detect active/finished/running tasks.
  """
  class MatchingError(Exception): pass

  def __init__(self, root):
    self._root_dir = root
    self._pathspec = TaskPath()

  def get_task_ids(self, state=None):
    paths = glob.glob(self._pathspec.given(root=self._root_dir,
                                           task_id="*",
                                           state=state or '*')
                                    .getpath('task_path'))
    path_re = re.compile(self._pathspec.given(root=re.escape(self._root_dir),
                                              task_id="(\S+)",
                                              state='(\S+)')
                                       .getpath('task_path'))
    for path in paths:
      try:
        task_state, task_id = path_re.match(path).groups()
      except:
        continue
      if state is None or task_state == state:
        yield (task_state, task_id)

  def get_process_runs(self, task_id, log_dir):
    paths = glob.glob(self._pathspec.given(root=self._root_dir,
                                           task_id=task_id,
                                           log_dir=log_dir,
                                           process='*',
                                           run='*')
                                    .getpath('process_logdir'))
    path_re = re.compile(self._pathspec.given(root=re.escape(self._root_dir),
                                              task_id=re.escape(task_id),
                                              log_dir=log_dir,
                                              process='(\S+)',
                                              run='(\d+)')
                                       .getpath('process_logdir'))
    for path in paths:
      try:
        process, run = path_re.match(path).groups()
      except:
        continue
      yield process, int(run)

  def get_process_logs(self, task_id, log_dir):
    for process, run in self.get_process_runs(task_id, log_dir):
      for logtype in ('stdout', 'stderr'):
        path = (self._pathspec.with_filename(logtype).given(root=self._root_dir,
                                                           task_id=task_id,
                                                           log_dir=log_dir,
                                                           process=process,
                                                           run=run)
                                                     .getpath('process_logdir'))
        if os.path.exists(path):
          yield path

  def get_checkpoint(self, task_id):
    return self._pathspec.given(root=self._root_dir, task_id=task_id).getpath('runner_checkpoint')

  def get_process_checkpoints(self, task_id):
    matching_paths = glob.glob(self._pathspec.given(root=self._root_dir,
                                                    task_id=task_id,
                                                    process='*')
                                             .getpath('process_checkpoint'))
    path_re = re.compile(self._pathspec.given(root=re.escape(self._root_dir),
                                              task_id=re.escape(task_id),
                                              process='(\S+)')
                                       .getpath('process_checkpoint'))
    for path in matching_paths:
      try:
        process, = path_re.match(path).groups()
      except:
        continue
      yield path
