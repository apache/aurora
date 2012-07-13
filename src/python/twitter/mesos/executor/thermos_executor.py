import getpass
import json
import os
import pwd
import signal
import sys
import tempfile
import threading
import time

# mesos
import mesos

from twitter.common import app, log
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

# thermos
from twitter.mesos.config.schema import MesosTaskInstance
from twitter.mesos.executor.task_runner_wrapper import (
  ProductionTaskRunner,
  AngrybirdTaskRunner)
from twitter.mesos.executor.executor_base import ThermosExecutorBase

# thrifts
from gen.twitter.mesos.ttypes import AssignedTask
from thrift.TSerialization import deserialize as thrift_deserialize

from .status_manager import StatusManager


app.configure(module='twitter.common_internal.app.modules.chickadee_handler',
    service_name='thermos_executor')
app.configure(debug=True)


if 'ANGRYBIRD_THERMOS' in os.environ:
  RUNNER_CLASS = AngrybirdTaskRunner
  LogOptions.set_log_dir(os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos/log'))
else:
  LogOptions.set_log_dir('/var/log/mesos')
  RUNNER_CLASS = ProductionTaskRunner


class ThermosExecutor(ThermosExecutorBase):
  def __init__(self, runner_class=RUNNER_CLASS):
    ThermosExecutorBase.__init__(self)
    self._runner = None
    self._task_id = None
    self._manager = None
    self._runner_class = runner_class

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
    self.log('launchTask got task: %s:%s' % (task.name, task.task_id.value))

    if self._runner:
      # TODO(wickman) Send LOST immediately for both tasks?
      log.error('Error!  Already running a task! %s' % self._runner)
      self.send_update(driver, self._task_id, 'LOST',
          "Task already running on this executor: %s" % self._task_id)
      return

    self._slave_id = task.slave_id.value
    self._task_id = task.task_id.value

    try:
      assigned_task = ThermosExecutor.deserialize_assigned_task(task)
      mesos_task, portmap = ThermosExecutor.deserialize_thermos_task(assigned_task)
    except Exception as e:
      log.fatal('Could not deserialize AssignedTask: %s' % e)
      self.send_update(driver, self._task_id, 'FAILED', "Could not deserialize task: %s" % e)
      driver.stop()
      return

    self.send_update(driver, self._task_id, 'STARTING', 'Initializing sandbox.')

    self._runner = self._runner_class(self._task_id, mesos_task, mesos_task.role().get(), portmap)
    self._runner.start()

    # TODO(wickman)  This should be able to timeout.  Send TASK_LOST after 60 seconds of trying?
    log.debug('Waiting for task to start.')
    while not self._runner.is_started():
      log.debug('   - sleeping...')
      time.sleep(Amount(250, Time.MILLISECONDS).as_(Time.SECONDS))
    log.debug('Task started.')
    self.send_update(driver, self._task_id, 'RUNNING')

    self._manager = StatusManager(self._runner, driver, self._task_id, portmap.get('health'))
    self._manager.start()

  def killTask(self, driver, task_id):
    self.log('killTask() got task_id: %s' % task_id)
    if self._runner is None:
      log.error('Got killTask but no task running!')
      return
    if task_id.value != self._task_id:
      log.error('Got killTask for a different task than what we are running!')
      return
    if self.thermos_status_is_terminal(self._runner.task_state()):
      log.error('Got killTask for task in terminal state!')
      return
    log.info('Issuing kills.')
    self._runner.kill()
    self._runner.quitquitquit()

  def shutdown(self, driver):
    self.log('shutdown()')
    if self._task_id:
      self.killTask(driver, self._task_id)


def main():
  LogOptions.set_disk_log_level('DEBUG')
  thermos_executor = ThermosExecutor()
  drv = mesos.MesosExecutorDriver(thermos_executor)
  drv.run()
  log.info('MesosExecutorDriver.run() has finished.')


app.main()
