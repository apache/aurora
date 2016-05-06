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

import getpass
import grp
import os
import pwd
from abc import abstractmethod, abstractproperty

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
  def _get_sandbox_user(self, assigned_task):
    return assigned_task.task.job.role

  @abstractmethod
  def from_assigned_task(self, assigned_task):
    """Return the appropriate Sandbox implementation from an AssignedTask."""


class DefaultSandboxProvider(SandboxProvider):
  SANDBOX_NAME = 'sandbox'

  def from_assigned_task(self, assigned_task):
    container = assigned_task.task.container
    if container.docker or container.mesos and container.mesos.image:
      return FileSystemImageSandbox(self.SANDBOX_NAME, self._get_sandbox_user(assigned_task))
    else:
      return DirectorySandbox(
        os.path.abspath(self.SANDBOX_NAME),
        self._get_sandbox_user(assigned_task))


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

    try:
      safe_mkdir(self.root)
    except (IOError, OSError) as e:
      raise self.CreationError('Failed to create the sandbox: %s' % e)

    if self._user:
      try:
        pwent = pwd.getpwnam(self._user)
        grent = grp.getgrgid(pwent.pw_gid)
      except KeyError:
        raise self.CreationError(
            'Could not create sandbox because user does not exist: %s' % self._user)

      try:
        log.debug('DirectorySandbox: chown %s:%s %s' % (self._user, grent.gr_name, self.root))
        os.chown(self.root, pwent.pw_uid, pwent.pw_gid)
        log.debug('DirectorySandbox: chmod 700 %s' % self.root)
        os.chmod(self.root, 0700)
      except (IOError, OSError) as e:
        raise self.CreationError('Failed to chown/chmod the sandbox: %s' % e)

  def destroy(self):
    try:
      safe_rmtree(self.root)
    except (IOError, OSError) as e:
      raise self.DeletionError('Failed to destroy sandbox: %s' % e)


class FileSystemImageSandbox(DirectorySandbox):
  """
  A sandbox implementation that configures the sandbox correctly for tasks provisioned from a
  filesystem image.
  """

  MESOS_DIRECTORY_ENV_VARIABLE = 'MESOS_DIRECTORY'
  MESOS_SANDBOX_ENV_VARIABLE = 'MESOS_SANDBOX'

  def __init__(self, sandbox_name, user=None):
    self._mesos_host_sandbox = os.environ[self.MESOS_DIRECTORY_ENV_VARIABLE]
    self._root = os.path.join(self._mesos_host_sandbox, sandbox_name)
    super(FileSystemImageSandbox, self).__init__(self._root, user=user)

  def _create_symlinks(self):
    # This sets up the container to have a similar directory structure to the host.
    # It takes self._mesos_host_sandbox (e.g. "[exec-root]/runs/RUN1/") and:
    #   - Sets mesos_host_sandbox_root = "[exec-root]/runs/" (one level up from mesos_host_sandbox)
    #   - Creates mesos_host_sandbox_root (recursively)
    #   - Symlinks _mesos_host_sandbox -> $MESOS_SANDBOX (typically /mnt/mesos/sandbox)
    # $MESOS_SANDBOX is provided in the environment by the Mesos containerizer.

    mesos_host_sandbox_root = os.path.dirname(self._mesos_host_sandbox)
    try:
      safe_mkdir(mesos_host_sandbox_root)
      os.symlink(os.environ[self.MESOS_SANDBOX_ENV_VARIABLE], self._mesos_host_sandbox)
    except (IOError, OSError) as e:
      raise self.CreationError('Failed to create the sandbox root: %s' % e)

  def create(self):
    self._create_symlinks()
    super(FileSystemImageSandbox, self).create()
