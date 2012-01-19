import getpass
import os
import pwd
import signal
import subprocess
import tempfile
import threading
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
from twitter.mesos.config.schema import MesosTaskInstance

from twitter.mesos.executor.task_runner_wrapper import (
  ProductionTaskRunner,
  AngrybirdTaskRunner)

# thrifts
from gen.twitter.mesos.ttypes import AssignedTask
from gen.twitter.thermos.ttypes import TaskState
from thrift.TSerialization import deserialize as thrift_deserialize

app.configure(module='twitter.common.app.modules.exception_handler',
    enable=True, category='thermos_executor_exceptions')
app.configure(debug=True)

if 'ANGRYBIRD_HOME' in os.environ:
  RUNNER_CLASS = AngrybirdTaskRunner
else:
  RUNNER_CLASS = ProductionTaskRunner

class ExecutorPollingThread(threading.Thread):
  def __init__(self, runner, driver, task_id):
    self._driver = driver
    self._runner = runner
    self._task_id = task_id
    threading.Thread.__init__(self)

  def run(self):
    # wait for the runner process to finish
    while self._runner.is_alive():
      time.sleep(Amount(500, Time.MILLISECONDS).as_(Time.SECONDS))

    state = self._runner.state()

    finish_state = None
    if state.state == TaskState.ACTIVE:
      log.error("Runner is dead but task state unexpectedly ACTIVE!")
      self._runner.quitquitquit()
      finish_state = mesos_pb.TASK_FAILED
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


class ThermosExecutor(mesos.Executor):
  def __init__(self):
    self._runner = None
    self._slave_id = None
    self._task_id = None
    self._poller = None

  def init(self, driver, args):
    self._log('init()')

  def _log(self, msg):
    log.info('Executor [%s]: %s' % (self._slave_id, msg))

  @staticmethod
  def task_update(task_id, state, message=None):
    update = mesos_pb.TaskStatus()
    update.task_id.value = task_id
    update.state = state
    if message:
      update.message = message
    return update

  @staticmethod
  def deserialize_assigned_task(task):
    """
      Deserialize task from a launchTask task protocol buffer.

      Returns:
        (task, portmap)
    """
    try:
      assigned_task = thrift_deserialize(AssignedTask(), task.data)
    except Exception as e:
      raise ValueError('Could not deserialize task! %s' % e)
    return assigned_task

  @staticmethod
  def deserialize_thermos_task(assigned_task):
    thermos_task = assigned_task.task.thermosConfig
    if not thermos_task:
      raise ValueError('Task did not have a thermosConfig!')
    try:
      json_blob = json.loads(thermos_task)
    except Exception as e:
      raise ValueError('Could not deserialize thermosConfig JSON! %s' % e)
    return (MesosTaskInstance(json_blob), assigned_task.assignedPorts)

  def launchTask(self, driver, task):
    if self._runner:
      log.error('Error!  Already running a task! %s' % self._runner)
      driver.sendStatusUpdate(
        self.task_update(self._task_id, mesos_pb.TASK_LOST,
          "Task already running on this executor: %s" % self._task_id))
      return

    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value
    self._log('launchTask() - Got task: %s:%s' % (task.name, task.task_id.value))

    try:
      assigned_task = ThermosExecutor.deserialize_assigned_task(task)
      thermos_task = ThermosExecutor.deserialize_thermos_task(assigned_task)
    except Exception as e:
      log.fatal('Could not deserialize AssignedTask: %s' % e)
      driver.sendStatusUpdate(self.task_update(self._task_id, mesos_pb.TASK_FAILED,
        "Could not deserialize task: %s" % e))
      driver.stop()
      return

    self._runner = RUNNER_CLASS(self._task_id, thermos_task, assigned_task.requestedPorts)
    self._runner.start()
    log.debug('Waiting for task to start.')

    # TODO(wickman)  This should be able to timeout.  Send TASK_LOST after 60 seconds of trying?
    while self._runner.state() is None:
      log.debug('   - sleeping...')
      time.sleep(Amount(250, Time.MILLISECONDS).as_(Time.SECONDS))
    log.debug('Task started.')

    update = mesos_pb.TaskStatus()
    update.task_id.value = task.task_id.value
    update.state = mesos_pb.TASK_RUNNING
    log.debug('Sending TASK_RUNNING update.')
    driver.sendStatusUpdate(update)

    self._poller = ExecutorPollingThread(self._runner, driver, self._task_id)
    self._poller.start()

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
