import re
import glob
from twitter.thermos.base import TaskPath

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class TaskDetector(object):
  """
    Helper class in front of TaskPath to detect active/finished/running tasks.
  """
  class MatchingError(Exception): pass

  def __init__(self, root):
    self._root_dir = root
    self._pathspec = TaskPath(root = root)

  def get_task_ids(self, state=None):
    paths = glob.glob(self._pathspec.given(task_id = "*", state = state or '*')
                                    .getpath('task_path'))
    path_re = re.compile(self._pathspec.given(task_id = "(\S+)", state = '(\S+)')
                                       .getpath('task_path'))
    for path in paths:
      try:
        task_state, task_id = path_re.match(path).groups()
      except:
        continue
      if state is None or task_state == state:
        yield (task_state, task_id)

  def get_checkpoint(self, task_id):
    return self._pathspec.given(task_id = task_id).getpath('runner_checkpoint')

  def get_process_checkpoints(self, task_id):
    matching_paths = glob.glob(self._pathspec.given(task_id=task_id, fork_time='*', pid='*')
                                             .getpath('process_checkpoint'))
    path_re = re.compile(self._pathspec.given(task_id=task_id, fork_time='(\d+)', pid='(\d+)')
                                 .getpath('process_checkpoint'))
    for path in matching_paths:
      try:
        fork_time, pid = path_re.match(path).groups()
      except:
        continue
      yield path
