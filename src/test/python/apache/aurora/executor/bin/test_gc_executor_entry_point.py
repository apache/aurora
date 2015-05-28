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

from mock import create_autospec, Mock, patch

from apache.aurora.executor.bin.gc_executor_main import initialize, proxy_main
from apache.aurora.executor.gc_executor import ThermosGCExecutor
from apache.thermos.monitoring.detector import ChainedPathDetector


def test_gc_executor_valid_import_dependencies():
  assert proxy_main is not None


class GcExecutorMainTest(unittest.TestCase):
  def test_chained_path_detector_initialized(self):
    mock_gc_executor = create_autospec(spec=ThermosGCExecutor)
    with patch('apache.aurora.executor.bin.gc_executor_main.ThermosGCExecutor',
        return_value=mock_gc_executor) as mock:
      with patch('apache.aurora.executor.bin.gc_executor_main.MesosExecutorDriver',
          return_value=Mock()):

        initialize()
        assert len(mock.mock_calls) == 1
        call = mock.mock_calls[0]
        _, args, _ = call
        assert len(args) == 1 and isinstance(args[0], ChainedPathDetector)
