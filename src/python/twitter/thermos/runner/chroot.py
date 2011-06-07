import os
import shutil

import twitter.common.log
log = twitter.common.log.get()

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

#  /var/run/thermos/{job.name}-{workflow.name}-{workflow.replica}/
class WorkflowChroot:
  """
    Encapsulate chroot.  AppApp integration will eventually go here.
  """

  CHROOT_SPEC = os.path.join("%(root)s", "%(id)s")

  def __init__(self, root, wf_id):
    self._root = root
    self._id   = wf_id
    self._path = WorkflowChroot.CHROOT_SPEC % { 'root': root, 'id': wf_id }

  # return path if creation is necessary
  def create_if_necessary(self):
    if self.created():
      return False
    else:
      return self.create()

  def created(self):
    return os.path.exists(self.path())

  def create(self):
    shutil.rmtree(self.path(), ignore_errors = True)
    try:
      os.makedirs(self.path(), 0700)
    except Exception, e:
      log.error('Unable to create dir %s!' % self.path())
      raise e
    return self.path()

  def path(self):
    return self._path
