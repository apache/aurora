import os
import shutil

from twitter.common import log

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class TaskChroot(object):
  """
    Encapsulate chroot.  AppApp integration will eventually go here.
  """

  CHROOT_SPEC = os.path.join("%(root)s", "%(id)s")

  def __init__(self, root, tsk_id):
    self._root = root
    self._id   = tsk_id
    self._path = TaskChroot.CHROOT_SPEC % { 'root': root, 'id': tsk_id }

  # return path if creation is necessary
  def create_if_necessary(self):
    if self.created():
      return False
    else:
      return self.create()

  def created(self):
    return os.path.exists(self.path())

  # TODO(wickman)  Use twitter.common.dirutil and get rid of the blanket exception catch.
  def create(self):
    shutil.rmtree(self.path(), ignore_errors = True)
    try:
      os.makedirs(self.path(), 0700)
    except Exception as e:
      log.error('Unable to create dir %s!' % self.path())
      raise e
    return self.path()

  def path(self):
    return self._path
