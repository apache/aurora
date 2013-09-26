from abc import abstractmethod, abstractproperty
import grp
import os
import pwd

from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_rmtree
from twitter.common.lang import Interface


class SandboxInterface(Interface):
  class Error(Exception): pass
  class CreationError(Error): pass
  class DeletionError(Error): pass

  @abstractproperty
  def root(self):
    """Return the root path of the sandbox."""

  @abstractmethod
  def exists(self):
    """Returns true if the sandbox appears to exist."""

  @abstractmethod
  def create(self, *args, **kw):
    """Create the sandbox."""

  @abstractmethod
  def destroy(self, *args, **kw):
    """Destroy the sandbox."""


class DirectorySandbox(SandboxInterface):
  """ Basic sandbox implementation using a directory on the filesystem """
  def __init__(self, root):
    self._root = root

  @property
  def root(self):
    return self._root

  def exists(self):
    return os.path.exists(self.root)

  def create(self, mesos_task):
    if mesos_task.has_layout():
      log.warning('DirectorySandbox got task with layout! %s' % mesos_task.layout())
    log.debug('DirectorySandbox: mkdir %s' % self.root)
    safe_mkdir(self.root)
    user = mesos_task.role().get()
    pwent = pwd.getpwnam(user)
    grent = grp.getgrgid(pwent.pw_gid)
    log.debug('DirectorySandbox: chown %s:%s %s' % (user, grent.gr_name, self.root))
    os.chown(self.root, pwent.pw_uid, pwent.pw_gid)
    log.debug('DirectorySandbox: chmod 700 %s' % self.root)
    os.chmod(self.root, 0700)

  def destroy(self):
    safe_rmtree(self.root)
