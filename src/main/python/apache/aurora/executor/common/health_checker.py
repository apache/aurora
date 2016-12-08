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

import math
import os
import pwd
import threading
import time
import traceback

from mesos.interface.mesos_pb2 import TaskState
from pystachio import Environment, String
from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import LambdaGauge

from apache.aurora.common.health_check.http_signaler import HttpSignaler
from apache.aurora.common.health_check.shell import ShellHealthCheck
from apache.aurora.config.schema.base import MesosContext
from apache.thermos.common.process_util import wrap_with_mesos_containerizer
from apache.thermos.config.schema import ThermosContext

from .status_checker import StatusChecker, StatusCheckerProvider, StatusResult
from .task_info import mesos_task_instance_from_assigned_task, resolve_ports

HTTP_HEALTH_CHECK = 'http'
SHELL_HEALTH_CHECK = 'shell'


class ThreadedHealthChecker(ExceptionalThread):
  """Perform a health check to determine if a service is healthy or not

    health_checker should be a callable returning a tuple of (boolean, reason), indicating
    respectively the health of the service and the reason for its failure (or None if the service is
    still healthy).

    Health-check failures are ignored during the first `math.ceil(grace_period_secs/interval_secs)`
    attempts. Status becomes `TASK_RUNNING` if `min_consecutive_successes` consecutive health
    check successes are seen, within `math.ceil(grace_period_secs/interval_secs) +
    min_consecutive_successes` attempts. (Converting time to attempts, accounts for slight
    discrepancies in sleep intervals do not cost an attempt, and unceremoniously end performing
    health checks and marking as unhealthy.)
  """

  def __init__(self,
      health_checker,
      sandbox,
      interval_secs,
      grace_period_secs,
      max_consecutive_failures,
      min_consecutive_successes,
      clock):
    """
    :param health_checker: health checker to confirm service health
    :type health_checker: function that returns (boolean, <string>)
    :param sandbox: Sandbox of the task corresponding to this health check.
    :type sandbox: DirectorySandbox
    :param interval_secs: delay between checks
    :type interval_secs: int
    :param grace_period_secs: initial period during which failed health-checks are ignored
    :type grace_period_secs: int
    :param max_consecutive_failures: number of failures to allow before marking dead
    :type max_consecutive_failures: int
    :param min_consecutive_successes: number of successes needed before marking healthy
    :type min_consecutive_successes: int
    :param clock: time module available to be mocked for testing
    :type clock: time module
    """
    self.checker = health_checker
    self.sandbox = sandbox
    self.clock = clock
    self.current_consecutive_failures = 0
    self.current_consecutive_successes = 0
    self.dead = threading.Event()
    self.interval = interval_secs
    self.max_consecutive_failures = max_consecutive_failures
    self.min_consecutive_successes = min_consecutive_successes
    self.snooze_file = None
    self.snoozed = False

    if self.sandbox and self.sandbox.exists():
      self.snooze_file = os.path.join(self.sandbox.root, '.healthchecksnooze')

    if grace_period_secs is not None:
      self.grace_period_secs = grace_period_secs
    else:
      self.grace_period_secs = interval_secs * 2

    self.attempts = 0
    # Compute the number of attempts that can be fit into the grace_period_secs,
    # to guarantee the number of health checks during the grace period.
    # Relying on time might cause non-deterministic behavior since the
    # health checks can be spaced apart by interval_secs + epsilon.
    self.forgiving_attempts = math.ceil(self.grace_period_secs / float(self.interval))

    # In the older version (without min_consecutive_successes) it is possible for a task
    # to make limping progress where the health checks fail all the time but never breach
    # the max_consecutive_failures limit and end up updated successfully.
    # Also a task can survive failures during initial_interval_secs and an additional
    # max_consecutive_failures and still update successfully.

    # Although initial_interval_secs is supposed to count for the task warm up time, to be
    # backward compatible add max_consecutive_failures to the max_attempts_to_running.
    self.max_attempts_to_running = (self.forgiving_attempts
        + self.max_consecutive_failures
        + self.min_consecutive_successes)
    self.running = False
    self.healthy, self.reason = True, None

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

  def _maybe_update_health_check_count(self, is_healthy, reason):
    if not is_healthy:
      log.warning('Health check failure: %s' % reason)

      if self.current_consecutive_successes > 0:
        log.debug('Reset consecutive successes counter.')
        self.current_consecutive_successes = 0

      if self._should_ignore_failure():
        return

      if self._should_fail_fast():
        log.warning('Not enough attempts left prove health, failing fast.')
        self.healthy = False
        self.reason = reason

      self.current_consecutive_failures += 1
      if self.current_consecutive_failures > self.max_consecutive_failures:
        log.warning('Reached consecutive failure limit.')
        self.healthy = False
        self.reason = reason
    else:
      self.current_consecutive_successes += 1

      if not self.running:
        if self.current_consecutive_successes >= self.min_consecutive_successes:
          log.info('Reached consecutive success limit.')
          self.running = True

      if self.current_consecutive_failures > 0:
        log.debug('Reset consecutive failures counter.')
        self.current_consecutive_failures = 0

  def _should_fail_fast(self):
    if not self.running:
      attempts_remaining = self.max_attempts_to_running - self.attempts
      successes_needed = self.min_consecutive_successes - self.current_consecutive_successes
      if attempts_remaining > 1 and successes_needed > attempts_remaining:
        return True
    return False

  def _should_ignore_failure(self):
    if self.attempts <= self.forgiving_attempts:
      log.warning('Ignoring failure of attempt: %s' % self.attempts)
      return True
    return False

  def _should_enforce_deadline(self):
    if not self.running:
      if self.attempts > self.max_attempts_to_running:
        return True
    return False

  def _do_health_check(self):
    if self._should_enforce_deadline():
      # This is needed otherwise it is possible to flap between
      # successful health-checks and failed health-checks, never
      # really satisfying the criteria for either healthy or unhealthy.
      log.warning('Exhausted attempts before satisfying liveness criteria.')
      self.healthy = False
      self.reason = 'Not enough successful health checks in time.'
      return self.healthy, self.reason

    is_healthy, reason = self._perform_check_if_not_disabled()
    if self.attempts <= self.max_attempts_to_running:
      self.attempts += 1
    self._maybe_update_health_check_count(is_healthy, reason)
    return is_healthy, reason

  def run(self):
    log.debug('Health checker thread started.')
    while not self.dead.is_set():
      is_healthy, reason = self._do_health_check()
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
               grace_period_secs=None,
               max_consecutive_failures=0,
               min_consecutive_successes=1,
               clock=time):
    self._health_checks = 0
    self._total_latency = 0
    self._stats_lock = threading.Lock()
    self._clock = clock
    self.threaded_health_checker = ThreadedHealthChecker(
        self._timing_wrapper(health_checker),
        sandbox,
        interval_secs,
        grace_period_secs,
        max_consecutive_failures,
        min_consecutive_successes,
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
    if self.threaded_health_checker.healthy:
      if self.threaded_health_checker.running:
        return StatusResult('Task is healthy.', TaskState.Value('TASK_RUNNING'))
      else:
        return StatusResult(None, TaskState.Value('TASK_STARTING'))
    return StatusResult('Failed health check! %s' % self.threaded_health_checker.reason,
        TaskState.Value('TASK_FAILED'))

  def name(self):
    return 'health_checker'

  def start(self):
    super(HealthChecker, self).start()
    self.threaded_health_checker.start()

  def stop(self):
    self.threaded_health_checker.stop()


class NoopHealthChecker(StatusChecker):
  """
     A health checker that will always report healthy status. This will be the
     stand-in health checker when no health checker is configured. Since there is
     no liveness requirement specified, the status is always `TASK_RUNNING`.
  """

  def __init__(self):
    self._status = StatusResult('No health-check defined, task is assumed healthy.',
        TaskState.Value('TASK_RUNNING'))

  @property
  def status(self):
    return self._status


class HealthCheckerProvider(StatusCheckerProvider):

  def __init__(self, nosetuid_health_checks=False, mesos_containerizer_path=None):
    self._nosetuid_health_checks = nosetuid_health_checks
    self._mesos_containerizer_path = mesos_containerizer_path

  @staticmethod
  def interpolate_cmd(task, cmd):
    """
    :param task: Assigned task passed from Mesos Agent
    :param cmd: Command defined inside shell_command inside config.
    :return: Interpolated cmd with filled in values, for example ports.
    """
    thermos_namespace = ThermosContext(
        task_id=task.taskId,
        ports=task.assignedPorts)
    mesos_namespace = MesosContext(instance=task.instanceId)
    command = String(cmd) % Environment(
        thermos=thermos_namespace,
        mesos=mesos_namespace
    )

    return command.get()

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

      # Filling in variables e.g. thermos.ports[http] that could have been passed in as part of
      # shell_command.
      interpolated_command = HealthCheckerProvider.interpolate_cmd(
        task=assigned_task,
        cmd=shell_command)

      # If we do not want the health check to execute as the user from the job's role
      # --nosetuid-health-checks should be passed as an argument to the executor.
      demote_to_job_role_user = None
      if not self._nosetuid_health_checks and not sandbox.is_filesystem_image:
        pw_entry = pwd.getpwnam(assigned_task.task.job.role)
        def demote_to_job_role_user():
          os.setgid(pw_entry.pw_gid)
          os.setuid(pw_entry.pw_uid)

      # If the task is executing in an isolated filesystem we'll want to wrap the health check
      # command within a mesos-containerizer invocation so that it's executed within that
      # filesystem.
      wrapper = None
      if sandbox.is_filesystem_image:
        health_check_user = (os.getusername() if self._nosetuid_health_checks
            else assigned_task.task.job.role)
        def wrapper(cmd):
          return wrap_with_mesos_containerizer(
              cmd,
              health_check_user,
              sandbox.container_root,
              self._mesos_containerizer_path)

      shell_signaler = ShellHealthCheck(
        cmd=interpolated_command,
        preexec_fn=demote_to_job_role_user,
        timeout_secs=timeout_secs,
        wrapper_fn=wrapper)
      a_health_checker = lambda: shell_signaler()
    else:
      portmap = resolve_ports(mesos_task, assigned_task.assignedPorts)
      if 'health' not in portmap:
        log.warning('No health-checks defined, will use a no-op health-checker.')
        return NoopHealthChecker()
      http_config = health_checker.get(HTTP_HEALTH_CHECK, {})
      http_endpoint = http_config.get('endpoint')
      http_expected_response = http_config.get('expected_response')
      http_expected_response_code = http_config.get('expected_response_code')

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
      grace_period_secs=health_check_config.get('initial_interval_secs'),
      max_consecutive_failures=health_check_config.get('max_consecutive_failures'),
      min_consecutive_successes=health_check_config.get('min_consecutive_successes'))

    return health_checker
