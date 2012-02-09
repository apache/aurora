import getpass
import json
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
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

# thermos
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
  LogOptions.set_log_dir(os.path.join(os.environ['ANGRYBIRD_HOME'], 'logs/thermos/log'))
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

    last_state = state.statuses[-1].state
    finish_state = None
    if last_state == TaskState.ACTIVE:
      log.error("Runner is dead but task state unexpectedly ACTIVE!")
      self._runner.quitquitquit()
      finish_state = mesos_pb.TASK_FAILED
    elif last_state == TaskState.SUCCESS:
      finish_state = mesos_pb.TASK_FINISHED
    elif last_state == TaskState.FAILED:
      finish_state = mesos_pb.TASK_FAILED
    elif last_state == TaskState.KILLED:
      log.error("Runner died but task is expectedly in KILLED state!")
      finish_state = mesos_pb.TASK_KILLED
    else:
      log.error("Unknown task state! %s" % TaskState._VALUES_TO_NAMES.get(last_state, '(unknown)'))
      finish_state = mesos_pb.TASK_FAILED

    update = mesos_pb.TaskStatus()
    update.task_id.value = self._task_id
    update.state = finish_state
    log.info('Sending terminal state update.')
    self._driver.sendStatusUpdate(update)

    # the executor is ephemeral and we just submitted a terminal task state, so shutdown
    log.info('Stopping executor.')
    self._driver.stop()


class ThermosExecutor(mesos.Executor):
  # Why doesn't pb2 provide this for us?
  _STATES = {
    mesos_pb.TASK_STARTING: 'STARTING',
    mesos_pb.TASK_RUNNING:  'RUNNING',
    mesos_pb.TASK_FINISHED: 'FINISHED',
    mesos_pb.TASK_FAILED:   'FAILED',
    mesos_pb.TASK_KILLED:   'KILLED',
    mesos_pb.TASK_LOST:     'LOST',
  }

  def __init__(self):
    self._runner = None
    self._slave_id = None
    self._task_id = None
    self._poller = None

  def init(self, driver, args):
    self._log('init()')

  def _log(self, msg):
    log.info('Executor [%s]: %s' % (self._slave_id, msg))

  def send_update(self, driver, task_id, state, message=None):
    assert state in ThermosExecutor._STATES
    update = mesos_pb.TaskStatus()
    update.task_id.value = task_id
    update.state = state
    update.message = str(message)
    self._log('Updating %s => %s' % (task_id, ThermosExecutor._STATES[state]))
    self._log('   Reason: %s' % message)
    driver.sendStatusUpdate(update)

  @staticmethod
  def deserialize_assigned_task(task):
    """
      Deserialize task from a launchTask task protocol buffer.
      Returns AssignedTask
    """
    try:
      assigned_task = thrift_deserialize(AssignedTask(), task.data)
    except Exception as e:
      raise ValueError('Could not deserialize task! %s' % e)
    return assigned_task

  @staticmethod
  def deserialize_thermos_task(assigned_task):
    """
      Deserialize MesosTaskInstance from a AssignedTask thrift.
      Returns twitter.mesos.config.schema.MesosTaskInstance and the map of
      assigned by the scheduler.
    """
    thermos_task = assigned_task.task.thermosConfig
    if not thermos_task:
      raise ValueError('Task did not have a thermosConfig!')
    try:
      json_blob = json.loads(thermos_task)
    except Exception as e:
      raise ValueError('Could not deserialize thermosConfig JSON! %s' % e)
    return (MesosTaskInstance(json_blob), assigned_task.assignedPorts)

  def launchTask(self, driver, task):
    self._log('launchTask got task: %s:%s' % (task.name, task.task_id.value))

    if self._runner:
      # TODO(wickman) Send LOST immediately for both tasks?
      log.error('Error!  Already running a task! %s' % self._runner)
      self.send_update(driver, self._task_id, mesos_pb.TASK_LOST,
          "Task already running on this executor: %s" % self._task_id)
      return

    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value

    try:
      assigned_task = ThermosExecutor.deserialize_assigned_task(task)
      mesos_task, portmap = ThermosExecutor.deserialize_thermos_task(assigned_task)
    except Exception as e:
      log.fatal('Could not deserialize AssignedTask: %s' % e)
      self.send_update(driver, self._task_id, mesos_pb.TASK_FAILED,
        "Could not deserialize task: %s" % e)
      driver.stop()
      return

    self.send_update(driver, self._task_id, mesos_pb.TASK_STARTING, 'Initializing sandbox.')

    self._runner = RUNNER_CLASS(self._task_id, mesos_task, mesos_task.role().get(), portmap)
    self._runner.start()

    # TODO(wickman)  This should be able to timeout.  Send TASK_LOST after 60 seconds of trying?
    log.debug('Waiting for task to start.')
    while self._runner.state() is None:
      log.debug('   - sleeping...')
      time.sleep(Amount(250, Time.MILLISECONDS).as_(Time.SECONDS))
    log.debug('Task started.')
    self.send_update(driver, self._task_id, mesos_pb.TASK_RUNNING)

    self._poller = ExecutorPollingThread(self._runner, driver, self._task_id)
    self._poller.start()

  def killTask(self, driver, task_id):
    self._log('killTask() got task_id: %s' % task_id)
    if self._runner is None:
      log.error('Got killTask but no task running!')
      return
    if task_id.value != self._task_id:
      log.error('Got killTask for a different task than what we are running!')
      return
    if self._runner.state().state in (TaskState.SUCCESS, TaskState.FAILED, TaskState.KILLED):
      log.error('Got killTask for task in terminal state!')
      return
    log.info('Issuing kills.')
    self._runner.kill()

  def frameworkMessage(self, driver, message):
    self._log('frameworkMessage() - message: %s' % message)

  def shutdown(self, driver):
    self._log('shutdown()')
    if self._task_id:
      self.killTask(driver, self._task_id)

  def error(self, driver, code, message):
    self._log('error() - code: %s, message: %s' % (code, message))


LogOptions.set_log_dir('/var/log/thermos')
def main():
  LogOptions.set_disk_log_level('DEBUG')
  thermos_executor = ThermosExecutor()
  drv = mesos.MesosExecutorDriver(thermos_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')


app.main()
