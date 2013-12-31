from twitter.common import log

from gen.apache.aurora.constants import TERMINAL_STATES as AURORA_TERMINAL_STATES
from gen.apache.aurora.ttypes import ScheduleStatus
from gen.apache.thermos.ttypes import TaskState

import mesos
import mesos_pb2 as mesos_pb


class ThermosExecutorBase(mesos.Executor):
  # Statuses are hard, let's go shopping.
  MESOS_STATES = {
      mesos_pb.TASK_STARTING: 'STARTING',
      mesos_pb.TASK_RUNNING: 'RUNNING',
      mesos_pb.TASK_FINISHED: 'FINISHED',
      mesos_pb.TASK_FAILED: 'FAILED',
      mesos_pb.TASK_KILLED: 'KILLED',
      mesos_pb.TASK_LOST: 'LOST',
  }

  THERMOS_TO_MESOS_STATES = {
      TaskState.ACTIVE: mesos_pb.TASK_RUNNING,
      TaskState.SUCCESS: mesos_pb.TASK_FINISHED,
      TaskState.FAILED: mesos_pb.TASK_FAILED,
      TaskState.KILLED: mesos_pb.TASK_KILLED,
      TaskState.LOST: mesos_pb.TASK_LOST,
  }

  THERMOS_TO_TWITTER_STATES = {
      TaskState.ACTIVE: ScheduleStatus.RUNNING,
      TaskState.CLEANING: ScheduleStatus.RUNNING,
      TaskState.FINALIZING: ScheduleStatus.RUNNING,
      TaskState.SUCCESS: ScheduleStatus.FINISHED,
      TaskState.FAILED: ScheduleStatus.FAILED,
      TaskState.KILLED: ScheduleStatus.KILLED,
      TaskState.LOST: ScheduleStatus.LOST,
  }

  @staticmethod
  def twitter_status_is_terminal(status):
    return status in AURORA_TERMINAL_STATES

  @staticmethod
  def mesos_status_is_terminal(status):
    return status in (
        mesos_pb.TASK_FAILED,
        mesos_pb.TASK_FINISHED,
        mesos_pb.TASK_KILLED,
        mesos_pb.TASK_LOST,
    )

  @staticmethod
  def thermos_status_is_terminal(status):
    return status in (
        TaskState.FAILED,
        TaskState.KILLED,
        TaskState.LOST,
        TaskState.SUCCESS,
    )

  def __init__(self):
    self._slave_id = None

  def log(self, msg):
    log.info('Executor [%s]: %s' % (self._slave_id, msg))

  def registered(self, driver, executor_info, framework_info, slave_info):
    self.log('registered() called with:')
    self.log('   ExecutorInfo:  %s' % executor_info)
    self.log('   FrameworkInfo: %s' % framework_info)
    self.log('   SlaveInfo:     %s' % slave_info)
    self._driver = driver
    self._executor_info = executor_info
    self._framework_info = framework_info
    self._slave_info = slave_info

  def reregistered(self, driver, slave_info):
    self.log('reregistered() called with:')
    self.log('   SlaveInfo:     %s' % slave_info)

  def disconnected(self, driver):
    self.log('disconnected() called')

  def send_update(self, driver, task_id, state, message=None):
    update = mesos_pb.TaskStatus()
    if not isinstance(state, int):
      raise TypeError('Invalid state type %s, should be int.' % type(state))
    if state not in self.MESOS_STATES:
      raise ValueError('Invalid state: %s' % state)
    update.state = state
    update.task_id.value = task_id
    if message:
      update.message = str(message)
    self.log('Updating %s => %s' % (task_id, self.MESOS_STATES[state]))
    self.log('   Reason: %s' % message)
    driver.sendStatusUpdate(update)

  def frameworkMessage(self, driver, message):
    self.log('frameworkMessage() got message: %s, ignoring.' % message)

  def error(self, driver, message):
    self.log('Received error message: %s' % message)
