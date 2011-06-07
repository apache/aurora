import re, glob
from twitter.thermos.base import WorkflowPath

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class WorkflowDetector_Error(Exception): pass
class WorkflowDetector:
  """
    Helper class in front of WorkflowPath to detect active/finished/running workflows.
  """
  def __init__(self, root):
    self._root_dir = root
    self._pathspec = WorkflowPath(root = root)

  @staticmethod
  def _get_uids(pathspec, job_type):
    path_glob = pathspec.given(job_uid = "*").getpath(job_type)
    path_re   = pathspec.given(job_uid = "(\S+)").getpath(job_type)

    matching_paths = glob.glob(path_glob)
    path_re        = re.compile(path_re)

    uids = []
    for path in matching_paths:
      matched_blobs = path_re.match(path).groups()
      if len(matched_blobs) != 1:
        raise WorkflowDetector_Error("Error matching blobs in %s" % path)
      uids.append(int(matched_blobs[0]))
    return uids

  def get_active_uids(self):
    return self._get_uids(self._pathspec, 'active_job_path')

  def get_finished_uids(self):
    return self._get_uids(self._pathspec, 'finished_job_path')

  def get_checkpoint(self, uid):
    return self._pathspec.given(job_uid = uid).getpath('runner_checkpoint')
