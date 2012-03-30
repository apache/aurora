# mesos
import mesos
import mesos_pb2 as mesos_pb
from twitter.common import app, log
from gen.twitter.mesos.ttypes import ScheduleStatus
from gen.twitter.thermos.ttypes import TaskState

app.configure(module='twitter.common.app.modules.scribe_exception_handler',
    category='test_thermos_executor_exceptions')
app.configure(debug=True)

class ThermosExecutorBase(mesos.Executor):
  MESOS_STATES = {
    'STARTING': mesos_pb.TASK_STARTING,
    'RUNNING': mesos_pb.TASK_RUNNING,
    'FINISHED': mesos_pb.TASK_FINISHED,
    'FAILED': mesos_pb.TASK_FAILED,
    'KILLED': mesos_pb.TASK_KILLED,
    'LOST': mesos_pb.TASK_LOST,
  }

  @staticmethod
  def twitter_status_is_terminal(status):
    return status in (ScheduleStatus.FAILED, ScheduleStatus.FINISHED, ScheduleStatus.KILLED,
      ScheduleStatus.LOST)

  @staticmethod
  def mesos_status_is_terminal(status):
    return status in (mesos_pb.TASK_FINISHED, mesos_pb.TASK_FAILED, mesos_pb.TASK_KILLED,
      mesos_pb.TASK_LOST)

  @staticmethod
  def thermos_status_is_terminal(status):
    return status in (TaskState.SUCCESS, TaskState.FAILED, TaskState.KILLED)

  def __init__(self):
    self._slave_id = None

  def log(self, msg):
    log.info('Executor [%s]: %s' % (self._slave_id, msg))

  def init(self, driver, executor_info):
    self.log('init() called with ExecutorInfo: %s' % executor_info)

  def send_update(self, driver, task_id, state, message=None):
    assert state in ThermosExecutorBase.MESOS_STATES
    update = mesos_pb.TaskStatus()
    update.task_id.value = task_id
    update.state = ThermosExecutorBase.MESOS_STATES[state]
    update.message = str(message)
    self.log('Updating %s => %s' % (task_id, state))
    self.log('   Reason: %s' % message)
    driver.sendStatusUpdate(update)

  def frameworkMessage(self, driver, message):
    self.log('frameworkMessage() got message: %s, ignoring.' % message)

  def error(self, driver, code, message):
    self.log('ERROR code: %s, message: %s' % (code, message))
