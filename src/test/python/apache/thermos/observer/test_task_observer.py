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

import unittest

from mock import create_autospec, patch
from twitter.common.quantity import Amount, Time

from apache.thermos.observer.detector import ObserverTaskDetector
from apache.thermos.observer.task_observer import TaskObserver


class TaskObserverTest(unittest.TestCase):
  def test_run_loop(self):
    """Test observer run loop."""
    mock_task_detector = create_autospec(spec=ObserverTaskDetector)
    with patch(
        "apache.thermos.observer.task_observer.ObserverTaskDetector",
        return_value=mock_task_detector) as mock_detector:
      with patch('threading._Event.wait') as mock_wait:

        run_count = 3
        interval = 15
        observer = TaskObserver(mock_detector, interval=Amount(interval, Time.SECONDS))
        observer.start()
        while len(mock_wait.mock_calls) < run_count:
          pass

        observer.stop()

        assert len(mock_task_detector.mock_calls) >= run_count
        assert len(mock_wait.mock_calls) >= run_count
        args = mock_wait.mock_calls[1][1]
        assert interval == args[0]
