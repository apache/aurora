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

import threading
import time

from mesos.interface.mesos_pb2 import TaskState
from twitter.common import log
from twitter.common.exceptions import ExceptionalThread

from apache.aurora.common.http_signaler import HttpSignaler

from .status_checker import StatusChecker, StatusCheckerProvider, StatusResult
from .task_info import mesos_task_instance_from_assigned_task, resolve_ports


class ThreadedHealthChecker(ExceptionalThread):
  """Perform a health check to determine if a service is healthy or not

    health_checker should be a callable returning a tuple of (boolean, reason), indicating
    respectively the health of the service and the reason for its failure (or None if the service is
    still healthy).
  """

  def __init__(self,
      health_checker,
      interval_secs,
      initial_interval_secs,
      max_consecutive_failures,
      clock):
    """
    :param health_checker: health checker to confirm service health
    :type health_checker: function that returns (boolean, <string>)
    :param interval_secs: delay between checks
    :type interval_secs: int
    :param initial_interval_secs: seconds to wait before starting checks
    :type initial_interval_secs: int
    :param max_consecutive_failures: number of failures to allow before marking dead
    :type max_consecutive_failures: int
    :param clock: time module available to be mocked for testing
    :type clock: time module
    """
    self.checker = health_checker
    self.clock = clock
    self.current_consecutive_failures = 0
    self.dead = threading.Event()
    self.interval = interval_secs
    self.max_consecutive_failures = max_consecutive_failures

    if initial_interval_secs is not None:
      self.initial_interval = initial_interval_secs
    else:
      self.initial_interval = interval_secs * 2

    if self.initial_interval > 0:
      self.healthy, self.reason = True, None
    else:
      self.healthy, self.reason = self.checker()
    super(ThreadedHealthChecker, self).__init__()
    self.daemon = True

  def _maybe_update_failure_count(self, is_healthy, reason):
    if not is_healthy:
      log.warning('Health check failure: %s' % reason)
      self.current_consecutive_failures += 1
      if self.current_consecutive_failures > self.max_consecutive_failures:
        log.warning('Reached consecutive failure limit.')
        self.healthy = False
        self.reason = reason
    else:
      if self.current_consecutive_failures > 0:
        log.debug('Reset consecutive failures counter.')
      self.current_consecutive_failures = 0

  def run(self):
    log.debug('Health checker thread started.')
    self.clock.sleep(self.initial_interval)
    log.debug('Initial interval expired.')
    while not self.dead.is_set():
      is_healthy, reason = self.checker()
      self._maybe_update_failure_count(is_healthy, reason)
      self.clock.sleep(self.interval)

  def start(self):
    ExceptionalThread.start(self)

  def stop(self):
    log.debug('Health checker thread stopped.')
    self.dead.set()


class HealthChecker(StatusChecker):
  """Generic StatusChecker-conforming class which uses a thread for arbitrary periodic health checks

    health_checker should be a callable returning a tuple of (boolean, reason), indicating
    respectively the health of the service and the reason for its failure (or None if the service is
    still healthy).
  """

  def __init__(self,
               health_checker,
               interval_secs=10,
               initial_interval_secs=None,
               max_consecutive_failures=0,
               clock=time):
    self.threaded_health_checker = ThreadedHealthChecker(
        health_checker,
        interval_secs,
        initial_interval_secs,
        max_consecutive_failures,
        clock)

  @property
  def status(self):
    if not self.threaded_health_checker.healthy:
      return StatusResult('Failed health check! %s' % self.threaded_health_checker.reason,
          TaskState.Value('TASK_FAILED'))

  def start(self):
    super(HealthChecker, self).start()
    self.threaded_health_checker.start()

  def stop(self):
    self.threaded_health_checker.stop()


class HealthCheckerProvider(StatusCheckerProvider):
  def from_assigned_task(self, assigned_task, _):
    mesos_task = mesos_task_instance_from_assigned_task(assigned_task)
    portmap = resolve_ports(mesos_task, assigned_task.assignedPorts)

    if 'health' not in portmap:
      return None

    health_check_config = mesos_task.health_check_config().get()
    http_signaler = HttpSignaler(
        portmap['health'],
        timeout_secs=health_check_config.get('timeout_secs'))
    health_checker = HealthChecker(
        http_signaler.health,
        interval_secs=health_check_config.get('interval_secs'),
        initial_interval_secs=health_check_config.get('initial_interval_secs'),
        max_consecutive_failures=health_check_config.get('max_consecutive_failures'))
    return health_checker
