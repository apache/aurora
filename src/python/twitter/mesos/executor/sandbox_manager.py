from abc import ABCMeta, abstractmethod
import os
from twitter.common_internal.appapp import AppFactory

from twitter.common import app, log
from twitter.common.dirutil import safe_mkdir, safe_rmtree

app.add_option('--sandbox_root', dest='sandbox_root', metavar='PATH',
               default='/var/lib/thermos',
               help="The path where we will create DirectorySandbox sandboxes. [default: %default]")


class SandboxBase(object):
  __metaclass__ = ABCMeta

  class CreationError(Exception):
    pass

  class DeletionError(Exception):
    pass

  def __init__(self, task_id, sandbox_root=None):
    self._sandbox_root = sandbox_root or app.get_options().sandbox_root

  @abstractmethod
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
  def __init__(self, task_id, sandbox_root=None):
    SandboxBase.__init__(self, task_id, sandbox_root)
    self._dir = os.path.join(self._sandbox_root, task_id)

  def root(self):
    return self._dir

  def exists(self):
    return os.path.exists(self._dir)

  def create(self, mesos_task):
    import grp, pwd
    if mesos_task.has_layout():
      log.warning('DirectorySandbox got task with layout! %s' % mesos_task.layout())
    log.debug('DirectorySandbox: mkdir %s' % self._dir)
    safe_mkdir(self._dir)
    user = mesos_task.role().get()
    pwent = pwd.getpwnam(user)
    grent = grp.getgrgid(pwent.pw_gid)
    log.debug('DirectorySandbox: chown %s:%s %s' % (user, grent.gr_name, self._dir))
    os.chown(self._dir, pwent.pw_uid, pwent.pw_gid)
    log.debug('DirectorySandbox: chmod 700 %s' % self._dir)
    os.chmod(self._dir, 0700)

  def destroy(self):
    safe_rmtree(self._dir)


class AppAppSandbox(SandboxBase):
  def __init__(self, task_id):
    SandboxBase.__init__(self, task_id)

    self._task_id = task_id
    self._app = AppFactory.get()
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

  def root(self):
    if self._layout is None:
      raise Exception("There is no layout associated with this sandbox!")
    return self._layout.root

  def create(self, mesos_task):
    from app.commands.layout_create import LayoutCreateError, NeedSuperuser
    if self._layout:
      raise SandboxBase.CreationError('Layout %s already exists!' % self._layout)
    kw = AppAppSandbox.layout_create_args_from_task(self._app, self._task_id, mesos_task)
    try:
      self._layout = self._app.layout_create(**kw)
    except (LayoutCreateError, NeedSuperuser) as e:
      log.fatal('Could not create app-app layout!')
      raise SandboxBase.CreationError('Could not create app-app layout!  %s' % e)
    self._layouts = [self._layout]

  def destroy(self):
    if not self._layouts:
      raise SandboxBase.DeletionError('No layout associated with %s!' % self._task_id)
    errors = []
    for layout in self._layouts:
      if not self._app.layout_destroy(layout_name=layout.name,
                                      layout_revision=layout.revision, force=True):
        errors.append('Could not destroy %s' % layout)
    if errors:
      raise SandboxBase.DeletionError('Failed:\n%s' % '\n'.join(errors))


class SandboxManager(object):
  class UnknownError(Exception):
    pass

  MANAGERS = {
    'DirectorySandbox': DirectorySandbox,
    'AppAppSandbox': AppAppSandbox
  }

  @staticmethod
  def get(*args, **kw):
    manager = app.get_options().sandbox_manager
    if manager in SandboxManager.MANAGERS:
      return SandboxManager.MANAGERS[manager](*args, **kw)
    raise SandboxManager.UnknownError('Unknown sandbox manager: %s' %
      app.get_options().sandbox_manager)

app.add_option('--sandbox_manager', dest='sandbox_manager',
               type="choice", choices=SandboxManager.MANAGERS.keys(),
               default='DirectorySandbox',
               help="The sandbox creation mechanism to use for the Thermos executor.  Can be one "
                    "of DirectorySandbox (for standard directory-based apps) or AppAppSandbox for "
                    "AppApp layouts. [default: %default]")
