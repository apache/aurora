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
from itertools import chain

from twitter.common import log

from apache.aurora.client.base import format_response

from .scheduler_mux import SchedulerMux

from gen.apache.aurora.api.ttypes import ResponseCode


class StatusMuxHelper(object):
  """Handles mux/demux logic of the getTasksWithoutConfigs RPC."""

  def __init__(self, scheduler, query_factory, scheduler_mux=None):
    self._scheduler = scheduler
    self._query_factory = query_factory
    self._scheduler_mux = scheduler_mux

  def get_tasks(self, instance_ids=None):
    """Routes call to either immediate direct or multiplexed threaded execution.

    Arguments:
    instance_ids -- optional list of instance IDs to query for.

    Returns a list of tasks.
    """
    log.debug('Querying instance statuses: %s' % instance_ids)

    if self._scheduler_mux is not None:
      return self._get_tasks_multiplexed(instance_ids)
    else:
      return self._get_tasks(self._query_factory(instance_ids))

  def _get_tasks_multiplexed(self, instance_ids=None):
    """Gets tasks via SchedulerMux.

    Arguments:
    instance_ids -- optional list of instance IDs to query for.

    Returns a list of tasks.
    """
    tasks = []
    include_ids = lambda id: id in instance_ids if instance_ids is not None else True

    log.debug('Batch getting task status: %s' % instance_ids)
    try:
      unfiltered_tasks = self._scheduler_mux.enqueue_and_wait(
        self._get_tasks,
        instance_ids if instance_ids else [],
        self._create_aggregated_query)
      tasks = [task for task in unfiltered_tasks if include_ids(task.assignedTask.instanceId)]
    except SchedulerMux.Error as e:
      log.error('Failed to query status for instances %s. Reason: %s' % (instance_ids, e))

    log.debug('Done batch getting task status: %s' % instance_ids)
    return tasks

  def _get_tasks(self, query):
    """Gets tasks directly via SchedulerProxy.

    Arguments:
    query -- TaskQuery instance.

    Returns a list of tasks.
    """
    try:
      resp = self._scheduler.getTasksWithoutConfigs(query)
    except IOError as e:
      log.error('IO Exception during scheduler call: %s' % e)
      return []

    tasks = []
    if resp.responseCode == ResponseCode.OK:
      tasks = resp.result.scheduleStatusResult.tasks

    log.debug(format_response(resp))
    return tasks

  def _create_aggregated_query(self, instance_id_lists):
    """Aggregates multiple instance_id lists into a single list.

    Arguments:
    instance_id_lists -- list of lists of int.
    """
    instance_ids = list(chain.from_iterable(instance_id_lists))
    log.debug('Aggregated instance ids to query status: %s' % instance_ids)
    return self._query_factory(instance_ids)
