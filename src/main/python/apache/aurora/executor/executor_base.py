#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import mesos
import mesos_pb2 as mesos_pb
from twitter.common import log


class ExecutorBase(mesos.Executor):
  # Statuses are hard, let's go shopping.
  STATES_TO_STR = {
      mesos_pb.TASK_STARTING: 'STARTING',
      mesos_pb.TASK_RUNNING: 'RUNNING',
      mesos_pb.TASK_FINISHED: 'FINISHED',
      mesos_pb.TASK_FAILED: 'FAILED',
      mesos_pb.TASK_KILLED: 'KILLED',
      mesos_pb.TASK_LOST: 'LOST',
  }

  TERMINAL_STATES = frozenset([
        mesos_pb.TASK_FAILED,
        mesos_pb.TASK_FINISHED,
        mesos_pb.TASK_KILLED,
        mesos_pb.TASK_LOST,
  ])

  @classmethod
  def status_is_terminal(cls, status):
    return status in cls.TERMINAL_STATES

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
    if state not in self.STATES_TO_STR:
      raise ValueError('Invalid state: %s' % state)
    update.state = state
    update.task_id.value = task_id
    if message:
      update.message = str(message)
    self.log('Updating %s => %s' % (task_id, self.STATES_TO_STR[state]))
    self.log('   Reason: %s' % message)
    driver.sendStatusUpdate(update)

  def frameworkMessage(self, driver, message):
    self.log('frameworkMessage() got message: %s, ignoring.' % message)

  def error(self, driver, message):
    self.log('Received error message: %s' % message)
