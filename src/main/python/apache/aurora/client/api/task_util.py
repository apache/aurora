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

from apache.aurora.client.base import format_response

from gen.apache.aurora.api.ttypes import ResponseCode


class StatusHelper(object):
  """Simple wrapper around getTasksWithoutConfigs RPC call."""

  def __init__(self, scheduler, query_factory):
    self._scheduler = scheduler
    self._query_factory = query_factory

  def get_tasks(self, instance_ids=None, retry=False):
    """Gets tasks from the scheduler.

    Arguments:
    instance_ids -- optional list of instance IDs to query for.
    retry -- optional boolean value indicating whether to retry the operation.

    Returns a list of tasks.
    """
    log.debug('Querying instance statuses: %s' % instance_ids)
    try:
      resp = self._scheduler.getTasksWithoutConfigs(self._query_factory(instance_ids), retry=retry)
    except IOError as e:
      log.error('IO Exception during scheduler call: %s' % e)
      return []

    tasks = []
    if resp.responseCode == ResponseCode.OK:
      tasks = resp.result.scheduleStatusResult.tasks

    log.debug(format_response(resp))
    return tasks
