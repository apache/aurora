#
# Copyright 2013 Apache Software Foundation
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

from apache.aurora.common.http_signaler import HttpSignaler

from .status_checker import (
    ExitState,
    StatusChecker,
    StatusCheckerProvider,
    StatusResult,
)
from .task_info import mesos_task_instance_from_assigned_task, resolve_ports

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread

class HealthCheckerThread(StatusChecker, ExceptionalThread):
  """Generic, StatusChecker-conforming thread for arbitrary periodic health checks

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
    self._checker = health_checker
    self._interval = interval_secs
    if initial_interval_secs is not None:
      self._initial_interval = initial_interval_secs
    else:
      self._initial_interval = interval_secs * 2
    self._current_consecutive_failures = 0
    self._max_consecutive_failures = max_consecutive_failures
    self._dead = threading.Event()
    if self._initial_interval > 0:
      self._healthy, self._reason = True, None
    else:
      self._healthy, self._reason = self._checker()
    self._clock = clock
    super(HealthCheckerThread, self).__init__()
    self.daemon = True

  @property
  def status(self):
    if not self._healthy:
      return StatusResult('Failed health check! %s' % self._reason, ExitState.FAILED)

  def run(self):
    log.debug('Health checker thread started.')
    self._clock.sleep(self._initial_interval)
    log.debug('Initial interval expired.')
    while not self._dead.is_set():
      self._maybe_update_failure_count(*self._checker())
      self._clock.sleep(self._interval)

  def _maybe_update_failure_count(self, is_healthy, reason):
    if not is_healthy:
      log.warning('Health check failure: %s' % reason)
      self._current_consecutive_failures += 1
      if self._current_consecutive_failures > self._max_consecutive_failures:
        log.warning('Reached consecutive failure limit.')
        self._healthy = False
        self._reason = reason
    else:
      if self._current_consecutive_failures > 0:
        log.debug('Reset consecutive failures counter.')
      self._current_consecutive_failures = 0

  def start(self):
    StatusChecker.start(self)
    ExceptionalThread.start(self)

  def stop(self):
    log.debug('Health checker thread stopped.')
    self._dead.set()


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
    health_checker = HealthCheckerThread(
        http_signaler.health,
        interval_secs=health_check_config.get('interval_secs'),
        initial_interval_secs=health_check_config.get('initial_interval_secs'),
        max_consecutive_failures=health_check_config.get('max_consecutive_failures'))
    return health_checker
