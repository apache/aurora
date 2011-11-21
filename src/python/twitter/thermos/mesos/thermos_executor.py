import os
import pwd
import signal
import subprocess
import tempfile
import time

# mesos
import mesos
import mesos_pb2 as mesos_pb

from twitter.common import app, log
from twitter.common.contextutil import temporary_file
from twitter.common.dirutil import chmod_plus_x, safe_mkdir
from twitter.common.http.mirror_file import MirrorFile
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordWriter

# thermos
from twitter.thermos.base import TaskPath
from twitter.thermos.runner import TaskRunner
from twitter.thermos.observer import TaskMonitor

# thrifts
from thrift.TSerialization import serialize as thrift_serialize
from thrift.TSerialization import deserialize as thrift_deserialize
from gen.twitter.mesos.ttypes import AssignedTask
from gen.twitter.thermos.ttypes import TaskState

app.add_option("--sandbox_root", dest = "sandbox_root", metavar = "PATH",
               default = "/var/lib/thermos",
               help = "the path root where we will spawn workflow sandboxes")
app.add_option("--checkpoint_root", dest = "checkpoint_root", metavar = "PATH",
               default = "/var/run/thermos",
               help = "the path where we will store workflow logs and checkpoints")

app.configure(module='twitter.common.app.modules.exception_handler',
    enable=True, category='thermos_executor_exceptions')
app.configure(debug=True)

# TODO(wickman):  Factor this out into a separate file
class TaskRunnerProcess(object):
  SVN_REPO = 'svn.twitter.biz'
  SVN_PATH = '/science-binaries/home/thermos'
  PEX_NAME = 'thermos_run.pex'
  TEMPDIR  = None

  class SandboxCreationError(Exception):
    pass

  class TaskError(Exception):
    pass

  def __init__(self, task):
    self._popen = None
    self._setup_task(task)    # set ._task, .task_id, ._role, and ._monitor
    self._make_sandbox()      # set ._sandbox
    self._setup_runner()      # set ._runner_pex

  def _setup_runner(self):
    if TaskRunnerProcess.TEMPDIR is None:
      TaskRunnerProcess.TEMPDIR = tempfile.mkdtemp()
    self._runner_pex = MirrorFile(
        TaskRunnerProcess.SVN_REPO,
        os.path.join(TaskRunnerProcess.SVN_PATH, TaskRunnerProcess.PEX_NAME),
        os.path.join(TaskRunnerProcess.TEMPDIR, TaskRunnerProcess.PEX_NAME),
        https=True)
    log.info('Acquiring runner pex: %s' % (
      'Success' if self._runner_pex.refresh() else 'Already up to date'))
    chmod_plus_x(self._runner_pex.filename())

  def _setup_task(self, task):
    """
      Extract TaskRunnerProcess state from a Mesos task protocol buffer
    """
    try:
      assigned_task = thrift_deserialize(AssignedTask(), task.data)
    # TODO(wickman) Less restrictive catch
    except Exception as e:
      raise TaskRunnerProcess.TaskError('Could not deserialize task! %s' % e)
    thermos_task = assigned_task.task.thermosConfig
    if not thermos_task:
      raise TaskRunnerProcess.TaskError('Task did not have a thermosConfig!')
    self._task = thermos_task
    self._task_id = task.task_id.value
    self._role = thermos_task.job.role
    self._monitor = TaskMonitor(TaskPath(root=app.get_options().checkpoint_root), self._task_id)
    # write serialized task to disk
    with temporary_file(cleanup=False) as fp:
      self._task_filename = fp.name
      rw = ThriftRecordWriter(fp)
      rw.write(self._task)
      rw.close()

  def _make_sandbox(self):
    sandbox = os.path.join(app.get_options().sandbox_root, self._task_id)
    safe_mkdir(sandbox)
    # TODO(wickman)  app-app integration ......HERE
    try:
      user = pwd.getpwnam(self._role)
    except KeyError:
      raise TaskRunnerProcess.SandboxCreationError("No such role: %s" % self._role)
    try:
      os.chown(sandbox, user.pw_uid, user.pw_gid)
    except OSError as e:
      raise TaskRunnerProcess.SandboxCreationError("Could not chown %s to role %s" % (
        sandbox, self._role))
    self._sandbox = sandbox

  def start(self):
    """
      Fork the task runner.
    """
    options = app.get_options()
    params = dict(log_dir = LogOptions.log_dir(),
                  checkpoint_root = options.checkpoint_root,
                  sandbox = self._sandbox,
                  task_id = self._task_id,
                  thermos_thrift = self._task_filename,
                  setuid = self._role)
    # TODO(wickman)  Implement chroot+setuid inside thermos_run.
    cmdline_args = [self._runner_pex.filename()]
    cmdline_args.extend('--%s=%s' % (flag, value) for flag, value in params.items())
    cmdline_args.extend([
      '--enable_scribe_exception_hook',
      '--scribe_exception_category=thermos_runner_exceptions',
      '--enable_chroot'])
    log.info('Forking off runner with cmdline: %s' % ' '.join(cmdline_args))
    self._popen = subprocess.Popen(cmdline_args)

  def state(self):
    return self._monitor.get_state()

  def is_alive(self):
    """
      Is the process underlying the Thermos task runner alive?
    """
    return self._popen is not None and self._popen.poll() is None

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
    runner = TaskRunner(self._task,
        app.get_options().sandbox_root,
        app.get_options().checkpoint_root,
        self._task_id)
    runner.kill()


class ThermosExecutor(mesos.Executor):
  def __init__(self):
    self._runner = None
    self._slave_id = None
    self._task_id = None
    self._killing = False

  def init(self, driver, args):
    self._log('init()')

  def _log(self, msg):
    log.info('Executor [%s]: %s' % (self._slave_id, msg))

  @staticmethod
  def _boilerplate_lost_task_update(task):
    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.slave_id.value = task.slave_id.value
    update.state = mesos_pb.TASK_LOST
    return update

  def launchTask(self, driver, task):
    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value
    self._log('launchTask() - Got task: %s:%s' % (task.name, task.task_id.value))

    if self._runner:
      log.error('Error!  Already running a task! %s' % self._runner)
      driver.sendStatusUpdate(self._boilerplate_lost_task_update(task))
      return

    self._runner = TaskRunnerProcess(task)
    self._runner.start()
    log.debug('Waiting for task to start.')

    # TODO(wickman)  This should be able to timeout.  Send TASK_LOST after 60 seconds of trying.
    while self._runner.state() is None:
      log.debug('   - sleeping...')
      time.sleep(Amount(250, Time.MILLISECONDS).as_(Time.SECONDS))
    log.debug('Task started.')

    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb.TASK_RUNNING
    log.debug('Sending TASK_RUNNING update.')
    driver.sendStatusUpdate(update)

    # wait for the runner process to finish
    while self._runner.is_alive():
      time.sleep(Amount(500, Time.MILLISECONDS).as_(Time.SECONDS))

    state = self._runner.state()

    finish_state = None
    if state.state == TaskState.ACTIVE:
      if not self._killing:
        # TODO(wickman)  Should probably attempt to rebind to the task a certain number of times.
        log.error("Runner is dead but task state unexpectedly ACTIVE!")
      self._runner.quitquitquit()
      finish_state = mesos_pb.TASK_KILLED
    elif state.state == TaskState.SUCCESS:
      finish_state = mesos_pb.TASK_FINISHED
    elif state.state == TaskState.FAILED:
      finish_state = mesos_pb.TASK_FAILED
    elif state.state == TaskState.KILLED:
      log.error("Runner died but task is expectedly in KILLED state!")
      finish_state = mesos_pb.TASK_KILLED
    else:
      log.error("Unknown task state!")
      finish_state = mesos_pb.TASK_FAILED

    update = mesos_pb.TaskStatus()
    update.task_id.value = self._task_id
    update.state = finish_state
    log.info('Sending terminal state update.')
    driver.sendStatusUpdate(update)

    # the executor is ephemeral and we just submitted a terminal task state, so shutdown
    log.info('Stopping executor.')
    driver.stop()

  def killTask(self, driver, task_id):
    self._log('killTask() - Got task_id: %s' % task_id)
    if self._runner is None:
      log.error('Got killTask but no task running!')
      return
    if task_id != self._task_id:
      log.error('Got killTask for a different task than what we are running!')
      return
    if self._runner.state().state != TaskState.ACTIVE:
      log.error('Got killTask for task in terminal state!')
      return
    log.info('Issuing kills.')
    self._killing = True
    self._runner.kill()

  def frameworkMessage(self, driver, message):
    self._log('frameworkMessage() - message: %s' % message)

  def shutdown(self, driver):
    self._log('shutdown()')

  def error(self, driver, code, message):
    self._log('error() - code: %s, message: %s' % (code, message))


def main():
  LogOptions.set_disk_log_level('DEBUG')
  thermos_executor = ThermosExecutor()
  drv = mesos.MesosExecutorDriver(thermos_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')
  time.sleep(10) # necessary due to ASF MESOS-36

app.main()
