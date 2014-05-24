#
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
from twitter.common.exceptions import ExceptionalThread
from twitter.common.quantity import Amount, Time


class ExecutorTimeout(ExceptionalThread):
  DEFAULT_TIMEOUT = Amount(10, Time.SECONDS)

  def __init__(self, event, driver, logger=log.error, timeout=DEFAULT_TIMEOUT):
    self._event = event
    self._logger = logger
    self._driver = driver
    self._timeout = timeout
    super(ExecutorTimeout, self).__init__()
    self.daemon = True

  def run(self):
    self._event.wait(self._timeout.as_(Time.SECONDS))
    if not self._event.is_set():
      self._logger('Executor timing out.')
      self._driver.stop()
