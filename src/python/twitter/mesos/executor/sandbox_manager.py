from abc import abstractmethod, abstractproperty
import os

from twitter.common import app, log
from twitter.common.dirutil import safe_mkdir, safe_rmtree
from twitter.common.lang import Interface
from twitter.common_internal.appapp import AppFactory

class SandboxBase(Interface):
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


class DirectorySandbox(SandboxBase):
  """ Basic sandbox implementation using a directory on the filesystem """
  def __init__(self, root):
    self._root = root

  @property
  def root(self):
    return self._root

  def exists(self):
    return os.path.exists(self.root)

  def create(self, mesos_task):
    import grp, pwd
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


class AppAppSandbox(SandboxBase):
  """ Sandbox implementation using an app-app layout as a sandbox """
  def __init__(self, task_id):
    self._task_id = task_id
    try:
      self._app = AppFactory.get()
    except AppFactory.UnsupportedPlatform as err:
      raise self.Error("Cannot initialize AppAppSandbox: %s" % err)
    self._layout = None
    self._layouts = self._app.layout_list(name=task_id)

    if len(self._layouts) > 0:
      self._layout = self._layouts[0]

  def exists(self):
    return len(self._layouts) > 0

  @staticmethod
  def layout_create_args_from_task(appobj, task_id, mesos_task):
    kw = {
      'name': task_id,
      'packages': [],
      'size': mesos_task.task().resources().disk().get()
    }

    layout = mesos_task.layout()

    for package in layout().packages():
      package_name, package_version = package.name().get(), package.version().get()
      kw['packages'].append('%s=%s' % (package_name, package_version))

    return kw

  @property
  def root(self):
    if self._layout is None:
      raise self.Error("There is no layout associated with this sandbox!")
    return self._layout.root

  def create(self, mesos_task):
    from app.commands.layout_create import LayoutCreateError, NeedSuperuser
    if self._layout:
      raise self.CreationError('Layout %s already exists!' % self._layout)
    kw = AppAppSandbox.layout_create_args_from_task(self._app, self._task_id, mesos_task)
    try:
      self._layout = self._app.layout_create(**kw)
    except (LayoutCreateError, NeedSuperuser) as e:
      log.fatal('Could not create app-app layout!')
      raise self.CreationError('Could not create app-app layout!  %s' % e)
    self._layouts = [self._layout]

  def destroy(self):
    if not self._layouts:
      raise self.DeletionError('No layout associated with %s!' % self._task_id)
    errors = []
    for layout in self._layouts:
      if not self._app.layout_destroy(layout_name=layout.name,
                                      layout_revision=layout.revision, force=True):
        errors.append('Could not destroy %s' % layout)
    if errors:
      raise self.DeletionError('Failed:\n%s' % '\n'.join(errors))
