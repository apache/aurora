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
import threading
from collections import defaultdict, namedtuple

from twitter.common.quantity import Amount, Time

from .error_handling_thread import spawn_worker

try:
  from Queue import Queue, Empty
except ImportError:
  from queue import Queue, Empty


class SchedulerMux(object):
  """Multiplexes scheduler RPC requests on a dedicated worker thread."""

  class Error(Exception):
    """Call error wrapper."""
    pass

  OK_RESULT = 1
  DEFAULT_WAIT_TIMEOUT = Amount(1, Time.SECONDS)
  DEFAULT_JOIN_TIMEOUT = Amount(5, Time.SECONDS)
  DEFAULT_RPC_TIMEOUT = Amount(120, Time.SECONDS)
  WORK_ITEM = namedtuple('WorkItem', ['completion_queue', 'command', 'data', 'aggregator'])

  def __init__(self, wait_timeout=DEFAULT_WAIT_TIMEOUT):
    self.__queue = Queue()
    self.__terminating = threading.Event()
    self.__wait_timeout = wait_timeout
    self.__worker = spawn_worker(self.__monitor)

  def __monitor(self):
    """Main body of the multiplexer thread.

    This method repeatedly polls the worker queue for new calls, and then
    dispatches them in batches to the scheduler.
    Callers are notified when their requests complete."""

    requests_by_command = defaultdict(list)
    while not self.__terminating.is_set():
      try:
        work_item = self.__queue.get(timeout=self.__wait_timeout.as_(Time.SECONDS))
        requests_by_command[work_item.command].append(work_item)
      except Empty:
        self.__call_and_notify(requests_by_command)
        requests_by_command = defaultdict(list)

  def __call_and_notify(self, requests_by_command):
    """Batch executes scheduler requests and notifies on completion.

    Takes a set of RPC requests grouped by command type, dispatches them to the scheduler,
    and then waits for the batched calls to complete. When a call is completed, its callers
    will be notified via the completion queue."""

    for command, work_items in requests_by_command.items():
      request = [item.data for item in work_items]
      request = work_items[0].aggregator(request) if work_items[0].aggregator else request
      result_status = self.OK_RESULT
      result_data = None
      try:
        result_data = command(request)
      except (self.Error, Exception) as e:
        result_status = e

      for work_item in work_items:
        work_item.completion_queue.put((result_status, result_data))

  def _enqueue(self, completion_queue, command, data, aggregator):
    """Queues up a scheduler call for a delayed (batched) completion.

    Arguments:
    completion_queue -- completion queue to notify caller on completion.
    command -- callback signature accepting a list of data.
    data -- single request data object to be batched with other similar requests.
    aggregator -- callback function for data aggregation.
    """
    self.__queue.put(self.WORK_ITEM(completion_queue, command, data, aggregator))

  def terminate(self):
    """Requests the SchedulerMux to terminate."""
    self.__terminating.set()
    self.__worker.join(timeout=self.DEFAULT_JOIN_TIMEOUT.as_(Time.SECONDS))

  def enqueue_and_wait(self, command, data, aggregator=None, timeout=DEFAULT_RPC_TIMEOUT):
    """Queues up the scheduler call and waits for completion.

    Arguments:
    command -- scheduler command to run.
    data -- data to query scheduler for.
    aggregator -- callback function for data aggregation.
    timeout -- amount of time to wait for completion.

    Returns the aggregated command call response. Response data decomposition is up to the caller.
    """
    try:
      completion_queue = Queue()
      self._enqueue(completion_queue, command, data, aggregator)
      result = completion_queue.get(timeout=timeout.as_(Time.SECONDS))
      result_status = result[0]
      if result_status != self.OK_RESULT and not self.__terminating.is_set():
        if isinstance(result_status, self.Error):
          raise result_status
        else:
          raise self.Error('Unknown error: %s' % result_status)
      return result[1]
    except Empty:
      raise self.Error('Failed to complete operation within %s' % timeout)
