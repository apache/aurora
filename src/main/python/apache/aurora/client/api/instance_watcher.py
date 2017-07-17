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
from threading import Event

from twitter.common import log

from .health_check import StatusHealthCheck
from .task_util import StatusHelper

from gen.apache.aurora.api.ttypes import ScheduleStatus, TaskQuery


class Instance(object):
  def __init__(self, birthday=None, finished=False):
    self.birthday = birthday
    self.finished = finished
    self.healthy = False

  def set_healthy(self, value):
    self.healthy = value
    self.finished = True

  def __str__(self):
    return ('[birthday=%s, healthy=%s, finished=%s]' % (self.birthday, self.healthy, self.finished))


class InstanceWatcher(object):
  def __init__(self,
               scheduler,
               job_key,
               watch_secs,
               health_check_interval_seconds,
               clock=time,
               terminating_event=None):

    self._scheduler = scheduler
    self._job_key = job_key
    self._watch_secs = watch_secs
    self._health_check_interval_seconds = health_check_interval_seconds
    self._clock = clock
    self._terminating = terminating_event or Event()
    self._status_helper = StatusHelper(self._scheduler, self._create_query)

  def watch(self, instance_ids, health_check=None):
    """Watches a set of instances and detects failures based on a delegated health check.

    Arguments:
    instance_ids -- set of instances to watch.

    Returns a set of instances that are considered failed.
    """
    log.info('Watching instances: %s' % instance_ids)
    instance_ids = set(instance_ids)
    health_check = health_check or StatusHealthCheck()

    instance_states = {}

    def finished_instances():
      return dict((s_id, s) for s_id, s in instance_states.items() if s.finished)

    def set_instance_healthy(instance_id, now):
      if instance_id not in instance_states:
        instance_states[instance_id] = Instance(now)
      instance = instance_states.get(instance_id)
      if now > (instance.birthday + self._watch_secs):
        log.info('Instance %s has been up and healthy for at least %d seconds' % (
          instance_id, self._watch_secs))
        instance.set_healthy(True)

    def set_instance_unhealthy(instance_id):
      log.info('Instance %s is unhealthy' % instance_id)
      if instance_id in instance_states:
        # An instance that was previously healthy and currently unhealthy has failed.
        instance_states[instance_id].set_healthy(False)
      else:
        # An instance never passed a health check (e.g.: failed before the first health check).
        instance_states[instance_id] = Instance(finished=True)

    while not self._terminating.is_set():
      running_tasks = self._status_helper.get_tasks(instance_ids, retry=True)
      now = self._clock.time()
      tasks_by_instance = dict((task.assignedTask.instanceId, task) for task in running_tasks)
      for instance_id in instance_ids:
        if instance_id not in finished_instances():
          running_task = tasks_by_instance.get(instance_id)
          if running_task is not None:
            task_healthy = health_check.health(running_task)
            if task_healthy:
              set_instance_healthy(instance_id, now)
            else:
              set_instance_unhealthy(instance_id)

      log.debug('Instances health: %s' % ['%s: %s' % val for val in instance_states.items()])

      # Return if all tasks are finished.
      if set(finished_instances().keys()) == instance_ids:
        return set([s_id for s_id, s in instance_states.items() if not s.healthy])

      self._terminating.wait(self._health_check_interval_seconds)

  def terminate(self):
    """Requests immediate termination of the watch cycle."""
    self._terminating.set()

  def _create_query(self, instance_ids):
    query = TaskQuery()
    query.jobKeys = set([self._job_key])
    query.statuses = set([ScheduleStatus.RUNNING])
    query.instanceIds = instance_ids
    return query
