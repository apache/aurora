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

from abc import abstractmethod, abstractproperty

from mesos.interface.mesos_pb2 import TaskState
from twitter.common import log
from twitter.common.lang import Interface
from twitter.common.metrics import Observable


class StatusResult(object):
  """
    Encapsulates a reason for failure and a status value from mesos.interface.mesos_pb2.TaskStatus.
    As mesos 0.20.0 uses protobuf 2.5.0, see the EnumTypeWrapper[1] docs for more information.

    https://code.google.com/p/protobuf/source/browse/tags/2.5.0/
        python/google/protobuf/internal/enum_type_wrapper.py
  """

  def __init__(self, reason, status):
    self._reason = reason
    if status not in TaskState.values():
      raise ValueError('Unknown task state: %r' % status)
    self._status = status

  @property
  def reason(self):
    return self._reason

  @property
  def status(self):
    return self._status

  def __repr__(self):
    return '%s(%r, status=%r)' % (
        self.__class__.__name__,
        self._reason,
        TaskState.Name(self._status))

  def __eq__(self, other):
    if isinstance(other, StatusResult):
      return self._status == other._status and self._reason == other._reason
    return False


class StatusChecker(Observable, Interface):
  """Interface to pluggable status checkers for the Aurora Executor."""

  @abstractproperty
  def status(self):
    """Return None under normal operations.  Return StatusResult to indicate status proposal."""

  def name(self):
    """Return the name of the status checker.  By default it is the class name.  Subclassable."""
    return self.__class__.__name__

  def start(self):
    """Invoked once the task has been started."""
    pass

  def stop(self):
    """Invoked once a non-None status has been reported."""
    pass


class StatusCheckerProvider(Interface):
  @abstractmethod
  def from_assigned_task(self, assigned_task, sandbox):
    """
    :param assigned_task:
    :type assigned_task: AssignedTask
    :param sandbox: Sandbox of the task corresponding to this status check.
    :type sandbox: DirectorySandbox
    :return: Instance of a HealthChecker.
    """
    pass


class Healthy(StatusChecker):
  @property
  def status(self):
    return None


class ChainedStatusChecker(StatusChecker):
  def __init__(self, status_checkers):
    self._status_checkers = status_checkers
    self._status = None
    if not all(isinstance(h_i, StatusChecker) for h_i in status_checkers):
      raise TypeError('ChainedStatusChecker must take an iterable of StatusCheckers.')
    super(ChainedStatusChecker, self).__init__()

  @property
  def status(self):
    """
      Return status that is computed from the statuses of the StatusCheckers. The computed status
      is based on the priority given below (in increasing order of priority).

      None             -> healthy (lowest-priority)
      TASK_RUNNING     -> healthy and running
      TASK_STARTING    -> healthy but still in starting
      Otherwise        -> unhealthy (highest-priority)
    """
    if not self._in_terminal_state():
      cur_status = None
      for status_checker in self._status_checkers:
        status_result = status_checker.status
        if status_result is not None:
          log.info('%s reported %s' % (status_checker.__class__.__name__, status_result))
          if not isinstance(status_result, StatusResult):
            raise TypeError('StatusChecker returned something other than a StatusResult: got %s' %
                type(status_result))
          if status_result.status == TaskState.Value('TASK_STARTING'):
            # TASK_STARTING overrides other statuses
            cur_status = status_result
          elif status_result.status == TaskState.Value('TASK_RUNNING'):
            if cur_status is None or cur_status == TaskState.Value('TASK_RUNNING'):
              # TASK_RUNNING needs consensus (None is also included)
              cur_status = status_result
          else:
            # Any other status leads to a terminal state
            self._status = status_result
            return self._status
      self._status = cur_status
    return self._status

  def _in_terminal_state(self):
    return (self._status is not None and
        self._status.status != TaskState.Value('TASK_RUNNING') and
        self._status.status != TaskState.Value('TASK_STARTING'))

  def start(self):
    for status_checker in self._status_checkers:
      status_checker.start()

  def stop(self):
    for status_checker in self._status_checkers:
      status_checker.stop()
