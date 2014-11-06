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

from threading import Event
from unittest import TestCase

import mock
from mesos.interface import ExecutorDriver
from twitter.common.quantity import Amount, Time

from apache.aurora.executor.common.executor_timeout import ExecutorTimeout


class TestExecutorTimeout(TestCase):
  def test_run(self):
    event = Event()
    mock_driver = mock.create_autospec(spec=ExecutorDriver, instance=True)
    event.set()
    executor_timeout = ExecutorTimeout(event, mock_driver, timeout=Amount(0, Time.SECONDS))
    executor_timeout.run()
    assert mock_driver.stop.call_count == 0

  def test_run_timeout(self):
    event = Event()
    mock_driver = mock.create_autospec(spec=ExecutorDriver, instance=True)
    executor_timeout = ExecutorTimeout(event, mock_driver, timeout=Amount(0, Time.SECONDS))
    executor_timeout.run()
    mock_driver.stop.assert_called_once_with()
