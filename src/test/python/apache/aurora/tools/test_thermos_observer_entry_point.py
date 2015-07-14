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
import os
import unittest

from mock import Mock, create_autospec, patch
from twitter.common.quantity import Amount, Time

from apache.aurora.tools.thermos_observer import initialize
from apache.thermos.observer.task_observer import TaskObserver


class ThermosObserverMainTest(unittest.TestCase):
  def test_initialize(self):
    expected_interval = Amount(15, Time.SECONDS)
    mock_options = Mock(spec_set=['root', 'mesos_root', 'polling_interval_secs'])
    mock_options.root = ''
    mock_options.mesos_root = os.path.abspath('.')
    mock_options.polling_interval_secs = int(expected_interval.as_(Time.SECONDS))
    mock_task_observer = create_autospec(spec=TaskObserver)
    with patch(
        'apache.aurora.tools.thermos_observer.TaskObserver',
        return_value=mock_task_observer) as mock_observer:

      initialize(mock_options)

      assert len(mock_observer.mock_calls) == 1
      args = mock_observer.mock_calls[0][2]
      assert expected_interval == args['interval']
