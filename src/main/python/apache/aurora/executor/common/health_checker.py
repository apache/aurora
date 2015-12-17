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

import os.path
import threading
import time
import traceback

from mesos.interface.mesos_pb2 import TaskState
from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import LambdaGauge

from apache.aurora.common.health_check.http_signaler import HttpSignaler
from apache.aurora.common.health_check.shell import ShellHealthCheck

from .status_checker import StatusChecker, StatusCheckerProvider, StatusResult
from .task_info import mesos_task_instance_from_assigned_task, resolve_ports

HTTP_HEALTH_CHECK = 'http'
SHELL_HEALTH_CHECK = 'shell'


class ThreadedHealthChecker(ExceptionalThread):
  """Perform a health check to determine if a service is healthy or not

    health_checker should be a callable returning a tuple of (boolean, reason), indicating
    respectively the health of the service and the reason for its failure (or None if the service is
    still healthy).
  """

  def __init__(self,
      health_checker,
      sandbox,
      interval_secs,
      initial_interval_secs,
      max_consecutive_failures,
      clock):
    """
    :param health_checker: health checker to confirm service health
    :type health_checker: function that returns (boolean, <string>)
    :param sandbox: Sandbox of the task corresponding to this health check.
    :type sandbox: DirectorySandbox
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
    self.sandbox = sandbox
    self.clock = clock
    self.current_consecutive_failures = 0
    self.dead = threading.Event()
    self.interval = interval_secs
    self.max_consecutive_failures = max_consecutive_failures
    self.snooze_file = None
    self.snoozed = False

    if self.sandbox and self.sandbox.exists():
      self.snooze_file = os.path.join(self.sandbox.root, '.healthchecksnooze')

    if initial_interval_secs is not None:
      self.initial_interval = initial_interval_secs
    else:
      self.initial_interval = interval_secs * 2

    if self.initial_interval > 0:
      self.healthy, self.reason = True, None
    else:
      self.healthy, self.reason = self._perform_check_if_not_disabled()
    super(ThreadedHealthChecker, self).__init__()
    self.daemon = True

  def _perform_check_if_not_disabled(self):
    if self.snooze_file and os.path.isfile(self.snooze_file):
      self.snoozed = True
      log.info("Health check snooze file found at %s. Health checks disabled.", self.snooze_file)
      return True, None

    self.snoozed = False
    log.debug("Health checks enabled. Performing health check.")

    try:
      return self.checker()
    except Exception as e:
      log.error('Internal error in health check:')
      log.error(traceback.format_exc())
      return False, 'Internal health check error: %s' % e

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
    if self.initial_interval > 0:
      self.clock.sleep(self.initial_interval)
    log.debug('Initial interval expired.')
    while not self.dead.is_set():
      is_healthy, reason = self._perform_check_if_not_disabled()
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

    Exported metrics:
      health_checker.consecutive_failures: Number of consecutive failures observed.  Resets
        to zero on successful health check.
      health_checker.snoozed: Returns 1 if the health checker is snoozed, 0 if not.
      health_checker.total_latency_secs: Total time waiting for the health checker to respond in
        seconds. To get average latency, use health_checker.total_latency / health_checker.checks.
      health_checker.checks: Total number of health checks performed.
  """

  def __init__(self,
               health_checker,
               sandbox=None,
               interval_secs=10,
               initial_interval_secs=None,
               max_consecutive_failures=0,
               clock=time):
    self._health_checks = 0
    self._total_latency = 0
    self._stats_lock = threading.Lock()
    self._clock = clock
    self.threaded_health_checker = ThreadedHealthChecker(
        self._timing_wrapper(health_checker),
        sandbox,
        interval_secs,
        initial_interval_secs,
        max_consecutive_failures,
        clock)
    self.metrics.register(LambdaGauge('consecutive_failures',
        lambda: self.threaded_health_checker.current_consecutive_failures))
    self.metrics.register(LambdaGauge('snoozed', lambda: int(self.threaded_health_checker.snoozed)))
    self.metrics.register(LambdaGauge('total_latency_secs', lambda: self._total_latency))
    self.metrics.register(LambdaGauge('checks', lambda: self._health_checks))

  def _timing_wrapper(self, closure):
    """A wrapper around the health check closure that times the health check duration."""
    def wrapper(*args, **kw):
      start = self._clock.time()
      success, failure_reason = closure(*args, **kw)
      stop = self._clock.time()
      with self._stats_lock:
        self._health_checks += 1
        self._total_latency += stop - start
      return (success, failure_reason)
    return wrapper

  @property
  def status(self):
    if not self.threaded_health_checker.healthy:
      return StatusResult('Failed health check! %s' % self.threaded_health_checker.reason,
          TaskState.Value('TASK_FAILED'))

  def name(self):
    return 'health_checker'

  def start(self):
    super(HealthChecker, self).start()
    self.threaded_health_checker.start()

  def stop(self):
    self.threaded_health_checker.stop()


class HealthCheckerProvider(StatusCheckerProvider):
  def from_assigned_task(self, assigned_task, sandbox):
    """
    :param assigned_task:
    :param sandbox:
    :return: Instance of a HealthChecker.
    """
    mesos_task = mesos_task_instance_from_assigned_task(assigned_task)
    health_check_config = mesos_task.health_check_config().get()
    health_checker = health_check_config.get('health_checker', {})
    timeout_secs = health_check_config.get('timeout_secs')
    if SHELL_HEALTH_CHECK in health_checker:
      shell_command = health_checker.get(SHELL_HEALTH_CHECK, {}).get('shell_command')
      shell_signaler = ShellHealthCheck(
        cmd=shell_command,
        timeout_secs=timeout_secs
      )
      a_health_checker = lambda: shell_signaler()
    else:
      portmap = resolve_ports(mesos_task, assigned_task.assignedPorts)
      if 'health' not in portmap:
        return None
      if HTTP_HEALTH_CHECK in health_checker:
        # Assume user has already switched over to the new config since we found the key.
        http_config = health_checker.get(HTTP_HEALTH_CHECK, {})
        http_endpoint = http_config.get('endpoint')
        http_expected_response = http_config.get('expected_response')
        http_expected_response_code = http_config.get('expected_response_code')
      else:
        # TODO (AURORA-1563): Remove this clause after we deprecate support for following keys
        # directly in HealthCheckConfig
        http_endpoint = health_check_config.get('endpoint')
        http_expected_response = health_check_config.get('expected_response')
        http_expected_response_code = health_check_config.get('expected_response_code')
      http_signaler = HttpSignaler(
        portmap['health'],
        timeout_secs=timeout_secs)
      a_health_checker = lambda: http_signaler(
        endpoint=http_endpoint,
        expected_response=http_expected_response,
        expected_response_code=http_expected_response_code
      )

    health_checker = HealthChecker(
      a_health_checker,
      sandbox,
      interval_secs=health_check_config.get('interval_secs'),
      initial_interval_secs=health_check_config.get('initial_interval_secs'),
      max_consecutive_failures=health_check_config.get('max_consecutive_failures'))

    return health_checker
