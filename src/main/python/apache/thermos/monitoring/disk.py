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

from twitter.common import log
from twitter.common.dirutil import du
from twitter.common.exceptions import ExceptionalThread
from twitter.common.lang import Lockable


class DiskCollectorThread(ExceptionalThread):
  """ Thread to calculate aggregate disk usage under a given path using a simple algorithm """

  def __init__(self, path):
    self.path = path
    self.value = None
    self.event = threading.Event()
    super(DiskCollectorThread, self).__init__()
    self.daemon = True

  def run(self):
    start = time.time()
    self.value = du(self.path)
    log.debug("DiskCollectorThread: finished collection of %s in %.1fms" % (
        self.path, 1000.0 * (time.time() - start)))
    self.event.set()

  def finished(self):
    return self.event.is_set()


class DiskCollector(Lockable):
  """ Spawn a background thread to sample disk usage """

  def __init__(self, root):
    self._root = root
    self._thread = None
    self._value = 0
    super(DiskCollector, self).__init__()

  @Lockable.sync
  def sample(self):
    """ Trigger collection of sample, if not already begun """
    if self._thread is None:
      self._thread = DiskCollectorThread(self._root)
      self._thread.start()

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
