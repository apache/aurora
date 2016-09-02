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
import subprocess
from abc import abstractmethod, abstractproperty

from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_rmtree
from twitter.common.lang import Interface

from gen.apache.aurora.api.constants import TASK_FILESYSTEM_MOUNT_POINT


class SandboxInterface(Interface):
  class Error(Exception): pass
  class CreationError(Error): pass
  class DeletionError(Error): pass

  @abstractproperty
  def root(self):
    """Return the root path of the sandbox within the host filesystem."""

  @abstractproperty
  def container_root(self):
    """Return the root path of the sandbox as it's visible to the running task."""

  @abstractproperty
  def chrooted(self):
    """Returns whether or not the sandbox is a chroot."""

  @abstractproperty
  def is_filesystem_image(self):
    """Returns whether or not the task is using a filesystem image."""

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
  MESOS_DIRECTORY_ENV_VARIABLE = 'MESOS_DIRECTORY'

  def from_assigned_task(self, assigned_task, **kwargs):
    sandbox_root = os.path.join(os.environ[self.MESOS_DIRECTORY_ENV_VARIABLE], self.SANDBOX_NAME)

    container = assigned_task.task.container
    if container.docker:
      return DockerDirectorySandbox(sandbox_root, **kwargs)
    elif container.mesos and container.mesos.image:
      return FileSystemImageSandbox(
          sandbox_root,
          user=self._get_sandbox_user(assigned_task),
          **kwargs)
    else:
      return DirectorySandbox(sandbox_root, user=self._get_sandbox_user(assigned_task), **kwargs)


class DirectorySandbox(SandboxInterface):
  """ Basic sandbox implementation using a directory on the filesystem """

  def __init__(self, root, user=getpass.getuser(), **kwargs):
    self._root = root
    self._user = user

  @property
  def root(self):
    return self._root

  @property
  def container_root(self):
    return self.root

  @property
  def chrooted(self):
    return False

  @property
  def is_filesystem_image(self):
    return False

  def exists(self):
    return os.path.exists(self.root)

  def get_user_and_group(self):
    try:
      pwent = pwd.getpwnam(self._user)
      grent = grp.getgrgid(pwent.pw_gid)

      return (pwent, grent)
    except KeyError:
      raise self.CreationError(
              'Could not create sandbox because user does not exist: %s' % self._user)

  def create(self):
    log.debug('DirectorySandbox: mkdir %s' % self.root)

    try:
      safe_mkdir(self.root)
    except (IOError, OSError) as e:
      raise self.CreationError('Failed to create the sandbox: %s' % e)

    if self._user:
      pwent, grent = self.get_user_and_group()

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


class DockerDirectorySandbox(DirectorySandbox):
  """ A sandbox implementation that configures the sandbox correctly for docker containers. """

  def __init__(self, root, **kwargs):
    self._mesos_host_sandbox = os.environ[DefaultSandboxProvider.MESOS_DIRECTORY_ENV_VARIABLE]

    # remove the user value from kwargs if it was set.
    kwargs.pop('user', None)

    super(DockerDirectorySandbox, self).__init__(root, user=None, **kwargs)

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
      os.symlink(os.environ['MESOS_SANDBOX'], self._mesos_host_sandbox)
    except (IOError, OSError) as e:
      raise self.CreationError('Failed to create the sandbox root: %s' % e)

  def create(self):
    self._create_symlinks()
    super(DockerDirectorySandbox, self).create()


class FileSystemImageSandbox(DirectorySandbox):
  """
  A sandbox implementation that configures the sandbox correctly for tasks provisioned from a
  filesystem image.
  """

  # returncode from a `useradd` or `groupadd` call indicating that the uid/gid already exists.
  _USER_OR_GROUP_ID_EXISTS = 4

  # returncode from a `useradd` or `groupadd` call indicating that the user/group name
  # already exists.
  _USER_OR_GROUP_NAME_EXISTS = 9

  def __init__(self, root, **kwargs):
    self._task_fs_root = os.path.join(
        os.environ[DefaultSandboxProvider.MESOS_DIRECTORY_ENV_VARIABLE],
        TASK_FILESYSTEM_MOUNT_POINT)

    self._no_create_user = kwargs.pop('no_create_user', False)
    self._mounted_volume_paths = kwargs.pop('mounted_volume_paths', None)
    self._sandbox_mount_point = kwargs.pop('sandbox_mount_point', None)

    if self._sandbox_mount_point is None:
      raise self.Error(
          'Failed to initialize FileSystemImageSandbox: no value specified for sandbox_mount_point')

    super(FileSystemImageSandbox, self).__init__(root, **kwargs)

  def _verify_group_match_in_taskfs(self, group_id, group_name):
    try:
      result = subprocess.check_output(
          ['chroot', self._task_fs_root, 'getent', 'group', group_name])
    except subprocess.CalledProcessError as e:
      raise self.CreationError(
          'Error when getting group id for name %s in task image: %s' % (
          group_name, e))
    splitted = result.split(':')
    if (len(splitted) < 3 or splitted[0] != '%s' % group_name or
        splitted[2] != '%s' % group_id):
      raise self.CreationError(
          'Group id result %s from image does not match name %s and id %s' % (
          result, group_name, group_id))

  def _verify_user_match_in_taskfs(self, user_id, user_name, group_id, group_name):
    try:
      result = subprocess.check_output(
          ['chroot', self._task_fs_root, 'id', '%s' % user_name])
    except subprocess.CalledProcessError as e:
      raise self.CreationError(
          'Error when getting user id for name %s in task image: %s' % (
          user_name, e))

    expected_prefix = "uid=%s(%s) gid=%s(%s) groups=" % (user_id, user_name, group_id, group_name)
    if not result.startswith(expected_prefix):
      raise self.CreationError(
          'User group result %s from task image does not start with expected prefix %s' % (
          result, expected_prefix))

  def _create_user_and_group_in_taskfs(self):
    if self._user:
      pwent, grent = self.get_user_and_group()

      try:
        subprocess.check_call(
            ['groupadd', '-R', self._task_fs_root, '-g', '%s' % grent.gr_gid, grent.gr_name])
      except subprocess.CalledProcessError as e:
        # If the failure was due to the group existing, we're ok to continue, otherwise bail out.
        if e.returncode in [self._USER_OR_GROUP_ID_EXISTS, self._USER_OR_GROUP_NAME_EXISTS]:
          self._verify_group_match_in_taskfs(grent.gr_gid, grent.gr_name)
          log.info(
              'Group %s(%s) already exists in the task''s filesystem, no need to create.' % (
              grent.gr_name, grent.gr_gid))
        else:
          raise self.CreationError('Failed to create group in sandbox for task image: %s' % e)

      try:
        subprocess.check_call([
            'useradd',
            '-R',
            self._task_fs_root,
            '-u',
            '%s' % pwent.pw_uid,
            '-g', '%s' % pwent.pw_gid,
            pwent.pw_name])
      except subprocess.CalledProcessError as e:
        # If the failure was due to the user existing, we're ok to continue, otherwise bail out.
        if e.returncode in [self._USER_OR_GROUP_ID_EXISTS, self._USER_OR_GROUP_NAME_EXISTS]:
          self._verify_user_match_in_taskfs(
              pwent.pw_uid, pwent.pw_name, pwent.pw_gid, grent.gr_name)
          log.info(
              'User %s (%s) already exists in the task''s filesystem, no need to create.' % (
              self._user, pwent.pw_uid))
        else:
          raise self.CreationError('Failed to create user in sandbox for task image: %s' % e)

  def _mount_paths(self):
    def do_mount(source, destination):
      safe_mkdir(destination)
      log.info('Mounting %s into task filesystem at %s.' % (source, destination))

      # This mount call is meant to mimic what mesos does when mounting into the container. C.f.
      # https://github.com/apache/mesos/blob/c3228f3c3d1a1b2c145d1377185cfe22da6079eb/src/slave/containerizer/mesos/isolators/filesystem/linux.cpp#L521-L528
      subprocess.check_call([
          'mount',
          '-n',
          '--rbind',
          source,
          destination])

    if self._mounted_volume_paths is not None:
      for container_path in self._mounted_volume_paths:
        if container_path != TASK_FILESYSTEM_MOUNT_POINT:
          target = container_path.lstrip('/')
          do_mount(container_path, os.path.join(self._task_fs_root, target))

    do_mount(self.root, os.path.join(self._task_fs_root, self._sandbox_mount_point.lstrip('/')))

  @property
  def container_root(self):
    return self._sandbox_mount_point

  @property
  def is_filesystem_image(self):
    return True

  def create(self):
    if not self._no_create_user:
      self._create_user_and_group_in_taskfs()

    super(FileSystemImageSandbox, self).create()

    self._mount_paths()
