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

import time

from mesos.interface.mesos_pb2 import TaskState
from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time

from .common.status_checker import StatusChecker


class StatusManager(ExceptionalThread):
  """
    An agent that periodically checks the health of a task via StatusCheckers that
    provide HTTP health checking, resource consumption, etc.

    Invokes the user-supplied `running_callback` with the status, if the StatusChecker
    returns `TASK_RUNNING` as the status. `running_callback` is invoked only once during
    the first time `TASK_RUNNING` is reported. For any other non-None statuses other than
    `TASK_STARTING`, invokes the `unhealthy_callback` and terminates.
  """
  POLL_WAIT = Amount(500, Time.MILLISECONDS)

  def __init__(self, status_checker, running_callback, unhealthy_callback, clock=time):
    if not isinstance(status_checker, StatusChecker):
      raise TypeError('status_checker must be a StatusChecker, got %s' % type(status_checker))
    if not callable(running_callback):
      raise TypeError('running_callback needs to be callable!')
    if not callable(unhealthy_callback):
      raise TypeError('unhealthy_callback needs to be callable!')
    self._status_checker = status_checker
    self._running_callback = running_callback
    self._running_callback_dispatched = False
    self._unhealthy_callback = unhealthy_callback
    self._clock = clock
    super(StatusManager, self).__init__()
    self.daemon = True

  def run(self):
    while True:
      status_result = self._status_checker.status
      if status_result is not None:
        if status_result.status == TaskState.Value('TASK_RUNNING'):
          if not self._running_callback_dispatched:
            self._running_callback(status_result)
            self._running_callback_dispatched = True
        elif status_result.status != TaskState.Value('TASK_STARTING'):
          log.info('Status manager got unhealthy status: %s' % status_result)
          self._unhealthy_callback(status_result)
          break

      self._clock.sleep(self.POLL_WAIT.as_(Time.SECONDS))
