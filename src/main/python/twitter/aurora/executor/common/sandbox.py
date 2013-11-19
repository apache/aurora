from abc import abstractmethod, abstractproperty
import getpass
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

  @abstractproperty
  def chrooted(self):
    """Returns whether or not the sandbox is a chroot."""

  @abstractmethod
  def exists(self):
    """Returns true if the sandbox appears to exist."""

  @abstractmethod
  def create(self, *args, **kw):
    """Create the sandbox."""

  @abstractmethod
  def destroy(self, *args, **kw):
    """Destroy the sandbox."""


class SandboxProvider(Interface):
  @abstractmethod
  def from_assigned_task(self, assigned_task):
    """Return the appropriate Sandbox implementation from an AssignedTask."""


class DirectorySandbox(SandboxInterface):
  """ Basic sandbox implementation using a directory on the filesystem """
  def __init__(self, root, user=getpass.getuser()):
    self._root = root
    self._user = user

  @property
  def root(self):
    return self._root

  @property
  def chrooted(self):
    return False

  def exists(self):
    return os.path.exists(self.root)

  def create(self):
    log.debug('DirectorySandbox: mkdir %s' % self.root)
    safe_mkdir(self.root)
    pwent = pwd.getpwnam(self._user)
    grent = grp.getgrgid(pwent.pw_gid)
    log.debug('DirectorySandbox: chown %s:%s %s' % (self._user, grent.gr_name, self.root))
    os.chown(self.root, pwent.pw_uid, pwent.pw_gid)
    log.debug('DirectorySandbox: chmod 700 %s' % self.root)
    os.chmod(self.root, 0700)

  def destroy(self):
    safe_rmtree(self.root)
