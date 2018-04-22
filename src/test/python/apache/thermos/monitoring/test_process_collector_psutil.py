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

import mock
import psutil

from apache.thermos.monitoring.process import ProcessSample
from apache.thermos.monitoring.process_collector_psutil import ProcessTreeCollector


@mock.patch('psutil.Process.children', autospec=True, spec_set=True)
def test_process_tree_collector(mock_process_iter):
  collector = ProcessTreeCollector(None)
  mock_process_iter.side_effect = psutil.Error
  collector.sample()
  assert collector.value == ProcessSample.empty()
  mock_process_iter.side_effect = IOError
  collector.sample()
  assert collector.value == ProcessSample.empty()
