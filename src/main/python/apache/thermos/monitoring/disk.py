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

"""Sample disk usage under a particular path

This module provides threads which can be used to gather information on the disk utilisation
under a particular path.
"""

import threading
import time

import requests
from jmespath import compile
from twitter.common import log
from twitter.common.dirutil import du
from twitter.common.exceptions import ExceptionalThread
from twitter.common.lang import AbstractClass, Lockable
from twitter.common.quantity import Amount, Time


class AbstractDiskCollector(Lockable, AbstractClass):
  def __init__(self, root, settings=None):
    self._settings = settings
    self._root = root
    self._thread = None
    self._value = 0
    super(AbstractDiskCollector, self).__init__()

  @property
  @Lockable.sync
  def value(self):
    """ Retrieve value of disk usage """
    if self._thread is not None and self._thread.finished():
      self._value = self._thread.value
      self._thread = None
    return self._value

  @property
  @Lockable.sync
  def completed_event(self):
    """ Return a threading.Event that will block until an in-progress disk collection is complete,
    or block indefinitely otherwise. Use with caution! (i.e.: set a timeout) """
    if self._thread is not None:
      return self._thread.event
    else:
      return threading.Event()


class DuDiskCollectorThread(ExceptionalThread):
  """ Thread to calculate aggregate disk usage under a given path using a simple algorithm """

  def __init__(self, path):
    self.value = None
    self.event = threading.Event()
    self._path = path
    super(DuDiskCollectorThread, self).__init__()
    self.daemon = True

  def run(self):
    start = time.time()
    self.value = du(self._path)
    log.debug("DuDiskCollectorThread: finished collection of %s in %.1fms",
              self._path, 1000.0 * (time.time() - start))
    self.event.set()

  def finished(self):
    return self.event.is_set()


class DuDiskCollector(AbstractDiskCollector):
  """ Spawn a background thread to sample disk usage """

  @Lockable.sync
  def sample(self):
    """ Trigger collection of sample, if not already begun """
    if self._thread is None:
      self._thread = DuDiskCollectorThread(self._root)
      self._thread.start()


class MesosDiskCollectorClient(ExceptionalThread):
  """ Thread to lookup disk usage under a given path from Mesos agent """

  DEFAULT_ERROR_VALUE = -1  # -1B

  def __init__(self, path, settings):
    self.value = None
    self.event = threading.Event()
    self._url = settings.http_api_url
    self._request_timeout = settings.disk_collection_timeout.as_(Time.SECONDS)
    self._path = path
    self._executor_key_expression = settings.executor_id_json_expression
    self._disk_usage_value_expression = settings.disk_usage_json_expression
    super(MesosDiskCollectorClient, self).__init__()
    self.daemon = True

  def run(self):
    start = time.time()
    response = self._request_agent_containers()
    filtered_container_stats = [
      container
      for container in response
      if str(self._executor_key_expression.search(container)) in self._path]

    if len(filtered_container_stats) != 1:
      self.value = self.DEFAULT_ERROR_VALUE
      log.warn("MesosDiskCollector: Didn't find container stats for path %s in agent metrics.",
               self._path)
    else:
      self.value = self._disk_usage_value_expression.search(filtered_container_stats[0])
      if self.value is None:
        self.value = self.DEFAULT_ERROR_VALUE
        log.warn("MesosDiskCollector: Didn't find disk usage stats for path %s in agent metrics.",
                 self._path)
      else:
        log.debug("MesosDiskCollector: finished collection of %s in %.1fms",
                  self._path, 1000.0 * (time.time() - start))

    self.event.set()

  def _request_agent_containers(self):
    try:
      resp = requests.get(self._url, timeout=self._request_timeout)
      resp.raise_for_status()
      return resp.json()
    except requests.exceptions.RequestException as ex:
      log.warn("MesosDiskCollector: Unexpected error talking to agent api: %s", ex)
      return []

  def finished(self):
    return self.event.is_set()


class MesosDiskCollector(AbstractDiskCollector):
  """ Spawn a background thread to lookup disk usage under a path using from Mesos agent """

  @Lockable.sync
  def sample(self):
    """ Trigger collection of sample, if not already begun """
    if self._thread is None:
      self._thread = MesosDiskCollectorClient(self._root, self._settings)
      self._thread.start()


class DiskCollectorSettings(object):
  """ Data container class to store Mesos agent api settings needed to retrive disk usages """

  DEFAULT_AGENT_CONTAINERS_ENDPOINT = "http://localhost:5051/containers"
  # Different versions of Mesos agent format their respons differntly. We use a json path library to
  # allow custom navigate through the json response object.
  # For documentaions see: http://jmespath.org/tutorial.html
  DEFAULT_EXECUTOR_ID_PATH = "executor_id"
  DEFAULT_DISK_USAGE_PATH = "statistics.disk_used_bytes"
  DEFAULT_DISK_COLLECTION_TIMEOUT = Amount(5, Time.SECONDS)
  DISK_COLLECTION_INTERVAL = Amount(60, Time.SECONDS)

  def __init__(
      self,
      http_api_url=DEFAULT_AGENT_CONTAINERS_ENDPOINT,
      executor_id_json_path=DEFAULT_EXECUTOR_ID_PATH,
      disk_usage_json_path=DEFAULT_DISK_USAGE_PATH,
      disk_collection_timeout=DEFAULT_DISK_COLLECTION_TIMEOUT,
      disk_collection_interval=DISK_COLLECTION_INTERVAL):

    self._http_api_url = http_api_url
    # We compile the JMESpath here for speed and also to detect bad JMESPaths immediately
    self._executor_id_json_expression = compile(executor_id_json_path)
    self._disk_usage_json_expression = compile(disk_usage_json_path)
    self._disk_collection_interval = disk_collection_interval
    self._disk_collection_timeout = disk_collection_timeout

  @property
  def http_api_url(self):
    return self._http_api_url

  @property
  def executor_id_json_expression(self):
    return self._executor_id_json_expression

  @property
  def disk_usage_json_expression(self):
    return self._disk_usage_json_expression

  @property
  def disk_collection_interval(self):
    return self._disk_collection_interval

  @property
  def disk_collection_timeout(self):
    return self._disk_collection_timeout
