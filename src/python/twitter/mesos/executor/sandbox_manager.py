from abc import ABCMeta, abstractmethod
import os
import sys

try:
  import app as appapp
  has_appapp = True
except ImportError:
  has_appapp = False

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
    pass

  @abstractmethod
  def create(self, *args, **kw):
    """Create the sandbox."""
    pass

  @abstractmethod
  def destroy(self, *args, **kw):
    """Destroy the sandbox."""
    pass


class DirectorySandbox(SandboxBase):
  def __init__(self, task_id, sandbox_root=None):
    SandboxBase.__init__(self, task_id, sandbox_root)
    self._dir = os.path.join(self._sandbox_root, task_id)

  def root(self):
    return self._dir

  def create(self, mesos_task):
    if len(mesos_task.layout().packages().get()) > 0:
      log.warning('DirectorySandbox got task with packages: %s'
                  % mesos_task.layout().packages())
    if len(mesos_task.layout().services().get()) > 0:
      log.warning('DirectorySandbox got task with services: %s'
                  % mesos_task.layout().services())
    safe_mkdir(self._dir)

  def destroy(self):
    safe_rmtree(self._dir)


class AppAppSandbox(SandboxBase):
  def __init__(self, task_id):
    SandboxBase.__init__(self, task_id)

    if not has_appapp:
      raise SandboxBase.CreationError("AppApp is unavailable: %r, PATH: %r" % (
        os.uname(), sys.path))

    self._task_id = task_id
    self._app = appapp.App()
    self._layout = None
    self._layouts = self._app.layout_list(layout_name=task_id)

    if len(self._layouts) > 0:
      self._layout = self._layouts[0]

  @staticmethod
  def layout_create_args_from_task(appobj, task_id, mesos_task):
    kw = {
      'name': task_id,
      'packages': [],
      'services': {},
      'size': mesos_task.task().resources().disk().get()
    }

    layout = mesos_task.layout()
    for service in layout().services():
      service_name = service.name().get()
      if service_name not in appobj.service_backends():
        raise SandboxBase.CreationError('Could not create layout: unknown Service: %s' % (
          service_name))
      kw['services'][appobj.service_backends()[service_name]] = (
        service.config().get() if service.has_config() else None)

    for package in layout().packages():
      package_name, package_version = package.name().get(), package.version().get()
      while True:
        if package_version == 'latest':
          found_packages = appobj.package_list(package_name=package_name, repo=True, limit=1)
        else:
          found_packages = appobj.package_list(package_name=package_name,
            package_revision=package_version, repo=True, limit=1)
        if len(found_packages) != 1:
          return SandboxBase.CreationError('Ambiguous package specification: %s' % package)
        pkg = found_packages[0]
        if pkg.package_path and not pkg.verify():
          log.warning('Package is cached locally but appears to be corrupt, uninstalling!')
          appobj.package_uninstall([pkg])
        else:
          break
      kw['packages'].append(pkg)

    return kw

  def root(self):
    if self._layout is None:
      raise Exception("There is no layout associated with this sandbox!")
    return self._layout.root

  def create(self, mesos_task):
    if self._layout:
      raise SandboxBase.CreationError('Layout %s already exists!' % self._layout)
    kw = AppAppSandbox.layout_create_args_from_task(self._app, self._task_id, mesos_task)
    try:
      self._layout, _ = self._app.layout_create(**kw)
    except Exception as e:
      log.fatal('Could not create layout!')
      raise SandboxBase.CreationError('Could not create layout!  %s' % e)
    self._layouts = [self._layout]

  def destroy(self):
    if not self._layouts:
      raise SandboxBase.DeletionError('No layout associated with %s!' % self._task_id)
    errors = []
    for layout in self._layouts:
      if not self._app.layout_destroy(layout_name=layout.name,
                                      layout_revision=layout.revision, force=True):
        errors.append('Could not destroy Layout(%s, %s)' % (layout.name, layout.revision))
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
