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

from twitter.common import log

from .health_check import StatusHealthCheck

from gen.apache.aurora.api.ttypes import Identity, ResponseCode, ScheduleStatus, TaskQuery


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
               restart_threshold,
               watch_secs,
               health_check_interval_seconds,
               clock=time):

    self._scheduler = scheduler
    self._job_key = job_key
    self._restart_threshold = restart_threshold
    self._watch_secs = watch_secs
    self._health_check_interval_seconds = health_check_interval_seconds
    self._clock = clock

  def watch(self, instance_ids, health_check=None):
    """Watches a set of instances and detects failures based on a delegated health check.

    Arguments:
    instance_ids -- set of instances to watch.

    Returns a set of instances that are considered failed.
    """
    log.info('Watching instances: %s' % instance_ids)
    instance_ids = set(instance_ids)
    health_check = health_check or StatusHealthCheck()
    now = self._clock.time()
    expected_healthy_by = now + self._restart_threshold
    max_time = now + self._restart_threshold + self._watch_secs

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

    def maybe_set_instance_unhealthy(instance_id, retriable):
      # An instance that was previously healthy and currently unhealthy has failed.
      if instance_id in instance_states:
        log.info('Instance %s is unhealthy' % instance_id)
        instance_states[instance_id].set_healthy(False)
      # If the restart threshold has expired or if the instance cannot be retried it is unhealthy.
      elif now > expected_healthy_by or not retriable:
        log.info('Instance %s was not reported healthy within %d seconds' % (
          instance_id, self._restart_threshold))
        instance_states[instance_id] = Instance(finished=True)

    while True:
      running_tasks = self._get_tasks_by_instance_id(instance_ids)
      now = self._clock.time()
      tasks_by_instance = dict((task.assignedTask.instanceId, task) for task in running_tasks)
      for instance_id in instance_ids:
        if instance_id not in finished_instances():
          running_task = tasks_by_instance.get(instance_id)
          if running_task is not None:
            task_healthy, retriable = health_check.health(running_task)
            if task_healthy:
              set_instance_healthy(instance_id, now)
            else:
              maybe_set_instance_unhealthy(instance_id, retriable)
          else:
            # Set retriable=True since an instance should be retried if it has not been healthy.
            maybe_set_instance_unhealthy(instance_id, retriable=True)

      log.debug('Instances health: %s' % ['%s: %s' % val for val in instance_states.items()])

      # Return if all tasks are finished.
      if set(finished_instances().keys()) == instance_ids:
        return set([s_id for s_id, s in instance_states.items() if not s.healthy])

      # Return if time is up.
      if now > max_time:
        return set([s_id for s_id in instance_ids if s_id not in instance_states
                                             or not instance_states[s_id].healthy])

      self._clock.sleep(self._health_check_interval_seconds)

  def _get_tasks_by_instance_id(self, instance_ids):
    log.debug('Querying instance statuses.')
    query = TaskQuery()
    query.owner = Identity(role=self._job_key.role)
    query.environment = self._job_key.environment
    query.jobName = self._job_key.name
    query.statuses = set([ScheduleStatus.RUNNING])

    query.instanceIds = instance_ids
    try:
      resp = self._scheduler.getTasksStatus(query)
    except IOError as e:
      log.error('IO Exception during scheduler call: %s' % e)
      return []

    tasks = []
    if resp.responseCode == ResponseCode.OK:
      tasks = resp.result.scheduleStatusResult.tasks

    log.debug('Response from scheduler: %s (message: %s)'
        % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
    return tasks
