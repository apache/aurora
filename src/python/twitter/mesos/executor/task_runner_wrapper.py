import errno
import getpass
import os
import signal
import subprocess
import tempfile

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.contextutil import temporary_file
from twitter.common.dirutil import chmod_plus_x, safe_rmtree
from twitter.common.http.mirror_file import MirrorFile

from twitter.thermos.base.path import TaskPath
from twitter.thermos.runner import TaskRunner
from twitter.thermos.monitoring.monitor import TaskMonitor
from twitter.thermos.config.loader import ThermosTaskWrapper

from twitter.mesos.executor.sandbox_manager import (
  AppAppSandbox,
  SandboxManager,
  DirectorySandbox)

app.add_option("--checkpoint_root", dest="checkpoint_root", metavar="PATH",
               default="/var/run/thermos",
               help="the path where we will store workflow logs and checkpoints")


class TaskRunnerWrapper(object):
  PEX_NAME = 'thermos_runner.pex'

  class TaskError(Exception):
    pass

  def __init__(self, task_id, mesos_task, role, mesos_ports, checkpoint_root=None):
    """
      :task_id       => task_id assigned by scheduler
      :mesos_task  => twitter.mesos.config.schema.MesosTaskInstance object
      :mesos_ports   => { name => port } dictionary
    """
    self._popen = None
    self._task_id = task_id
    self._mesos_task = mesos_task
    self._task = mesos_task.task()
    self._task_filename = TaskRunnerWrapper.dump_task(self._task)
    self._ports = mesos_ports
    self._checkpoint_root = checkpoint_root or app.get_options().checkpoint_root
    self._enable_chroot = False
    self._role = role

  @staticmethod
  def dump_task(task):
    with temporary_file(cleanup=False) as fp:
      filename = fp.name
      ThermosTaskWrapper(task).to_file(filename)
    return filename

  def start(self):
    """
      Fork the task runner.

      REQUIRES SUBCLASSES TO DEFINE:
        self._sandbox (SandboxManager)
        self._runner_pex (filename)
    """
    assert hasattr(self, '_sandbox')
    assert hasattr(self, '_runner_pex')
    assert os.path.exists(self._runner_pex)
    chmod_plus_x(self._runner_pex)

    self._monitor = TaskMonitor(TaskPath(root=self._checkpoint_root), self._task_id)

    try:
      log.info('Creating sandbox.')
      self._sandbox.create(self._mesos_task)
    except Exception as e:
      log.fatal('Could not construct sandbox: %s' % e)
      raise TaskRunnerWrapper.TaskError('Could not construct sandbox: %s' % e)

    params = dict(log_dir=LogOptions.log_dir(),
                  checkpoint_root=self._checkpoint_root,
                  sandbox=self._sandbox.root(),
                  task_id=self._task_id,
                  thermos_json=self._task_filename)

    if getpass.getuser() == 'root':
      params.update(setuid=self._role)

    cmdline_args = [self._runner_pex]
    cmdline_args.extend('--%s=%s' % (flag, value) for flag, value in params.items())
    if self._enable_chroot:
      cmdline_args.extend(['--enable_chroot'])
    for name, port in self._ports.items():
      cmdline_args.extend(['--port=%s:%s' % (name, port)])
    log.info('Forking off runner with cmdline: %s' % ' '.join(cmdline_args))
    self._popen = subprocess.Popen(cmdline_args)

  def state(self):
    return self._monitor.get_state()

  def task_state(self):
    return self._monitor.task_state()

  def is_started(self):
    return self._popen is not None

  def is_alive(self):
    """
      Is the process underlying the Thermos task runner alive?
    """
    if self._popen is None:
      return False

    # We can't rely upon subprocess.Popen.poll because we may be invoking
    # os.wait4 via TaskRunner.run => TaskRunnerHelper.reap_children.  While
    # arguably an abstraction breakdown, this workaround seems reasonable.
    try:
      pid, _ = os.waitpid(self._popen.pid, os.WNOHANG)
      if pid == 0:
        return True
    except OSError as e:
      if e.errno != errno.ECHILD:
        raise

    return False

  def cleanup(self):
    pass

  def kill(self):
    """
      Kill the underlying runner process.  Returns True if killed, False if
      it exited on its own.
    """
    assert self._popen is not None
    if self._popen.poll() is None:
      self._popen.send_signal(signal.SIGINT)
      self._popen.wait()
    return self._popen.poll() == -signal.SIGINT

  def quitquitquit(self):
    """Bind to the process tree of a Thermos task and kill it with impunity."""
    try:
      runner = TaskRunner.get(self._task_id, self._checkpoint_root)
      if runner:
        runner.kill(force=True)
      else:
        log.error('Could not instantiate runner!')
    except TaskRunner.Error as e:
      log.error('Could not quitquitquit runner: %s' % e)


class ProductionTaskRunner(TaskRunnerWrapper):
  def __init__(self, task_id, mesos_task, *args, **kwargs):
    TaskRunnerWrapper.__init__(self, task_id, mesos_task, *args, **kwargs)
    import pkg_resources
    import twitter.mesos.executor.resources
    self._tempdir = tempfile.mkdtemp()
    os.chmod(self._tempdir, 755)
    self._runner_pex = os.path.join(self._tempdir, self.PEX_NAME)
    with open(self._runner_pex, 'w') as fp:
      fp.write(pkg_resources.resource_stream(twitter.mesos.executor.resources.__name__,
        self.PEX_NAME).read())
    if mesos_task.has_layout():
      self._sandbox = AppAppSandbox(task_id)
      self._enable_chroot = True
    else:
      self._sandbox = DirectorySandbox(task_id)
      self._enable_chroot = False

  def cleanup(self):
    if self._tempdir:
      safe_rmtree(self._tempdir)
      self._tempdir = None


class AngrybirdTaskRunner(TaskRunnerWrapper):
  def __init__(self, task_id, *args, **kwargs):
    TaskRunnerWrapper.__init__(self, task_id, *args, **kwargs)
    self._angrybird_home = os.environ['ANGRYBIRD_HOME']
    self._angrybird_logdir = os.environ['ANGRYBIRD_THERMOS']
    self._sandbox_root = os.path.join(self._angrybird_logdir, 'thermos/lib')
    self._checkpoint_root = os.path.join(self._angrybird_logdir, 'thermos/run')
    self._runner_pex = os.path.join(self._angrybird_home, 'science', 'dist', self.PEX_NAME)
    self._sandbox = DirectorySandbox(task_id, self._sandbox_root)
