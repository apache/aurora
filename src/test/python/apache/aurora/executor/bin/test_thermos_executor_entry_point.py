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

from apache.aurora.executor.bin.thermos_executor_main import dump_runner_pex, initialize, proxy_main
from apache.aurora.executor.common.path_detector import MesosPathDetector
from apache.aurora.executor.thermos_task_runner import DefaultThermosTaskRunnerProvider


def test_thermos_executor_valid_import_dependencies():
  assert proxy_main is not None


class ThermosExecutorMainTest(unittest.TestCase):
  def test_checkpoint_path(self):
    mock_runner_provider = create_autospec(spec=DefaultThermosTaskRunnerProvider)
    mock_dump_runner_pex = create_autospec(spec=dump_runner_pex)
    mock_dump_runner_pex.return_value = Mock()
    mock_options = Mock()
    mock_options.execute_as_user = False
    mock_options.nosetuid = False
    with patch(
        'apache.aurora.executor.bin.thermos_executor_main.dump_runner_pex',
        return_value=mock_dump_runner_pex):
      with patch(
          'apache.aurora.executor.bin.thermos_executor_main.DefaultThermosTaskRunnerProvider',
          return_value=mock_runner_provider) as mock_provider:

        expected_path = os.path.join(os.path.abspath('.'), MesosPathDetector.DEFAULT_SANDBOX_PATH)
        thermos_executor = initialize(mock_options)

        assert thermos_executor is not None
        assert len(mock_provider.mock_calls) == 1
        args = mock_provider.mock_calls[0][1]
        assert len(args) == 2 and expected_path == args[1]
