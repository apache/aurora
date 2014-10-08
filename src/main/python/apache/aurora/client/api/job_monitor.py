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

from threading import Event

from twitter.common.quantity import Amount, Time

from .task_util import StatusMuxHelper

from gen.apache.aurora.api.constants import LIVE_STATES, TERMINAL_STATES
from gen.apache.aurora.api.ttypes import JobKey, TaskQuery


class JobMonitor(object):
  MIN_POLL_INTERVAL = Amount(2, Time.SECONDS)
  MAX_POLL_INTERVAL = Amount(150, Time.SECONDS)

  @classmethod
  def running_or_finished(cls, status):
    return status in (LIVE_STATES | TERMINAL_STATES)

  @classmethod
  def terminal(cls, status):
    return status in TERMINAL_STATES

  def __init__(self, scheduler, job_key, terminating_event=None,
               min_poll_interval=MIN_POLL_INTERVAL, max_poll_interval=MAX_POLL_INTERVAL,
               scheduler_mux=None):
    self._scheduler = scheduler
    self._job_key = job_key
    self._min_poll_interval = min_poll_interval
    self._max_poll_interval = max_poll_interval
    self._terminating = terminating_event or Event()
    self._status_helper = StatusMuxHelper(self._scheduler, self.create_query, scheduler_mux)

  def iter_tasks(self, instances):
    tasks = self._status_helper.get_tasks(instances)
    for task in tasks:
      yield task

  def states(self, instance_ids):
    states = {}
    for task in self.iter_tasks(instance_ids):
      status, instance_id = task.status, task.assignedTask.instanceId
      first_timestamp = task.taskEvents[0].timestamp
      if instance_id not in states or first_timestamp > states[instance_id][0]:
        states[instance_id] = (first_timestamp, status)
    return dict((instance_id, status[1]) for (instance_id, status) in states.items())

  def create_query(self, instances=None):
    return TaskQuery(
        jobKeys=[JobKey(role=self._job_key.role,
                        environment=self._job_key.env,
                        name=self._job_key.name)],
        instanceIds=frozenset([int(s) for s in instances]) if instances else None)

  def wait_until(self, predicate, instances=None, with_timeout=False):
    """Given a predicate (from ScheduleStatus => Boolean), wait until all requested instances
       return true for that predicate OR the timeout expires (if with_timeout=True)

    Arguments:
    predicate -- predicate to check completion with.
    instances -- optional subset of job instances to wait for.
    with_timeout -- if set, caps waiting time to the MAX_POLL_INTERVAL.

    Returns: True if predicate is met or False if timeout has expired.
    """
    poll_interval = self._min_poll_interval
    while not self._terminating.is_set() and not all(predicate(state) for state
        in self.states(instances).values()):

      if with_timeout and poll_interval >= self._max_poll_interval:
        return False

      self._terminating.wait(poll_interval.as_(Time.SECONDS))
      poll_interval = min(self._max_poll_interval, 2 * poll_interval)

    return True

  def terminate(self):
    """Requests immediate termination of the wait cycle."""
    self._terminating.set()
