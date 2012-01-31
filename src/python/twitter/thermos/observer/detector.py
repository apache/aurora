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

  @staticmethod
  def _get_task_ids(pathspec, task_type):
    path_glob = pathspec.given(task_id = "*").getpath(task_type)
    path_re   = pathspec.given(task_id = "(\S+)").getpath(task_type)

    matching_paths = glob.glob(path_glob)
    path_re        = re.compile(path_re)

    task_ids = []
    for path in matching_paths:
      matched_blobs = path_re.match(path).groups()
      if len(matched_blobs) != 1:
        raise TaskDetector.MatchingError("Error matching blobs in %s" % path)
      task_ids.append(matched_blobs[0])
    return task_ids

  def get_active_task_ids(self):
    return self._get_task_ids(self._pathspec, 'active_task_path')

  def get_finished_task_ids(self):
    return self._get_task_ids(self._pathspec, 'finished_task_path')

  def get_checkpoint(self, task_id):
    return self._pathspec.given(task_id = task_id).getpath('runner_checkpoint')
