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

from twitter.common import log

from .instance_watcher import InstanceWatcher
from .updater_util import FailureThreshold

from gen.apache.aurora.api.constants import ACTIVE_STATES
from gen.apache.aurora.api.ttypes import ResponseCode


class Restarter(object):
  def __init__(self,
               job_key,
               update_config,
               health_check_interval_seconds,
               scheduler,
               instance_watcher=None,
               lock=None):
    self._job_key = job_key
    self._update_config = update_config
    self.health_check_interval_seconds = health_check_interval_seconds
    self._scheduler = scheduler
    self._lock = lock
    self._instance_watcher = instance_watcher or InstanceWatcher(
        scheduler,
        job_key.to_thrift(),
        update_config.restart_threshold,
        update_config.watch_secs,
        health_check_interval_seconds)

  def restart(self, instances):
    # Verify that this operates on a valid job.
    query = self._job_key.to_thrift_query()
    query.statuses = ACTIVE_STATES
    status = self._scheduler.getTasksWithoutConfigs(query)
    if status.responseCode != ResponseCode.OK:
      return status

    failure_threshold = FailureThreshold(
        self._update_config.max_per_instance_failures,
        self._update_config.max_total_failures)

    if not instances:
      tasks = status.result.scheduleStatusResult.tasks

      instances = sorted(task.assignedTask.instanceId for task in tasks)
      if not instances:
        log.info("No instances specified, and no active instances found in job %s" % self._job_key)
        log.info("Nothing to do.")
        return status

    log.info("Performing rolling restart of job %s (instances: %s)" % (self._job_key, instances))

    while instances and not failure_threshold.is_failed_update():
      batch = instances[:self._update_config.batch_size]
      instances = instances[self._update_config.batch_size:]

      log.info("Restarting instances: %s", batch)

      resp = self._scheduler.restartShards(self._job_key.to_thrift(), batch, self._lock)
      if resp.responseCode != ResponseCode.OK:
        log.error('Error restarting instances: %s', resp.messageDEPRECATED)
        return resp

      failed_instances = self._instance_watcher.watch(batch)
      instances += failed_instances
      failure_threshold.update_failure_counts(failed_instances)

    if failure_threshold.is_failed_update():
      log.info("Restart failures threshold reached. Aborting")
    else:
      log.info("All instances were restarted successfully")

    return resp
