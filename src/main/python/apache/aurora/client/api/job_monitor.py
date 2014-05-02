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

import time

from gen.apache.aurora.api.constants import (
    LIVE_STATES,
    TERMINAL_STATES
)
from gen.apache.aurora.api.ttypes import (
    Identity,
    TaskQuery
)

from thrift.transport import TTransport
from twitter.common import log
from twitter.common.quantity import Amount, Time


class JobMonitor(object):
  MIN_POLL_INTERVAL = Amount(2, Time.SECONDS)
  MAX_POLL_INTERVAL = Amount(150, Time.SECONDS)

  @classmethod
  def running_or_finished(cls, status):
    return status in (LIVE_STATES | TERMINAL_STATES)

  @classmethod
  def terminal(cls, status):
    return status in TERMINAL_STATES

  def __init__(self, scheduler, job_key, clock=time,
               min_poll_interval=MIN_POLL_INTERVAL, max_poll_interval=MAX_POLL_INTERVAL):
    self._scheduler = scheduler
    self._job_key = job_key
    self._clock = clock
    self._min_poll_interval = min_poll_interval
    self._max_poll_interval = max_poll_interval

  def iter_query(self, query):
    try:
      res = self._scheduler.getTasksStatus(query)
    except TTransport.TTransportException as e:
      log.error('Failed to query tasks from scheduler: %s' % e)
      return
    if res is None or res.result is None:
      return
    for task in res.result.scheduleStatusResult.tasks:
      yield task

  def states(self, query):
    states = {}
    for task in self.iter_query(query):
      status, instance_id = task.status, task.assignedTask.instanceId
      first_timestamp = task.taskEvents[0].timestamp
      if instance_id not in states or first_timestamp > states[instance_id][0]:
        states[instance_id] = (first_timestamp, status)
    return dict((instance_id, status[1]) for (instance_id, status) in states.items())

  def create_query(self, instances=None):
    return TaskQuery(
        owner=Identity(role=self._job_key.role),
        environment=self._job_key.env,
        jobName=self._job_key.name,
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
    while not all(predicate(state) for state in self.states(self.create_query(instances)).values()):
      if with_timeout and poll_interval >= self._max_poll_interval:
        return False

      self._clock.sleep(poll_interval.as_(Time.SECONDS))
      poll_interval = min(self._max_poll_interval, 2 * poll_interval)

    return True