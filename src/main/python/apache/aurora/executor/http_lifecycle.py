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

from twitter.common import log
from twitter.common.quantity import Amount, Time

from apache.aurora.common.health_check.http_signaler import HttpSignaler

from .common.task_runner import TaskError, TaskRunner


class HttpLifecycleManager(TaskRunner):
  """A wrapper around a TaskRunner that performs HTTP lifecycle management."""

  DEFAULT_ESCALATION_WAIT = Amount(5, Time.SECONDS)
  WAIT_POLL_INTERVAL = Amount(1, Time.SECONDS)

  @classmethod
  def wrap(cls, runner, task_instance, portmap):
    """Return a task runner that manages the http lifecycle if lifecycle is present."""

    if not task_instance.has_lifecycle() or not task_instance.lifecycle().has_http():
      return runner

    http_lifecycle = task_instance.lifecycle().http()
    http_lifecycle_port = http_lifecycle.port().get()
    graceful_shutdown_wait_secs = (
        Amount(http_lifecycle.graceful_shutdown_wait_secs().get(), Time.SECONDS)
        if http_lifecycle.has_graceful_shutdown_wait_secs()
        else cls.DEFAULT_ESCALATION_WAIT)
    shutdown_wait_secs = (
        Amount(http_lifecycle.shutdown_wait_secs().get(), Time.SECONDS)
        if http_lifecycle.has_shutdown_wait_secs()
        else cls.DEFAULT_ESCALATION_WAIT)

    if not portmap or http_lifecycle_port not in portmap:
      # If DefaultLifecycle is ever to disable task lifecycle by default, we should
      # raise a TaskError here, since the user has requested http lifecycle without
      # binding a port to send lifecycle commands.
      return runner

    escalation_endpoints = [
        (http_lifecycle.graceful_shutdown_endpoint().get(), graceful_shutdown_wait_secs),
        (http_lifecycle.shutdown_endpoint().get(), shutdown_wait_secs)
    ]
    return cls(runner, portmap[http_lifecycle_port], escalation_endpoints)

  def __init__(self,
               runner,
               lifecycle_port,
               escalation_endpoints,
               clock=time):
    self._runner = runner
    self._lifecycle_port = lifecycle_port
    self._escalation_endpoints = escalation_endpoints
    self._clock = clock
    self.__started = False

  def _terminate_http(self):
    http_signaler = HttpSignaler(self._lifecycle_port)

    for endpoint, wait_time in self._escalation_endpoints:
      handled, _ = http_signaler(endpoint, use_post_method=True)
      log.info('Killing task, calling %s and waiting %s, handled is %s' % (
          endpoint, str(wait_time), str(handled)))

      waited = Amount(0, Time.SECONDS)
      while handled:
        if self._runner.status is not None:
          return True
        if waited >= wait_time:
          break

        self._clock.sleep(self.WAIT_POLL_INTERVAL.as_(Time.SECONDS))
        waited += self.WAIT_POLL_INTERVAL

  # --- public interface
  def start(self, timeout=None):
    self.__started = True
    return self._runner.start(timeout=timeout if timeout is not None else self._runner.MAX_WAIT)

  def stop(self, timeout=None):
    """Stop the runner.  If it's already completed, no-op.  If it's still running, issue a kill."""
    if not self.__started:
      raise TaskError('Failed to call TaskRunner.start.')

    log.info('Invoking runner HTTP teardown.')
    self._terminate_http()

    return self._runner.stop(timeout=timeout if timeout is not None else self._runner.MAX_WAIT)

  @property
  def status(self):
    """Return the StatusResult of this task runner.  This returns None as
       long as no terminal state is reached."""
    return self._runner.status
