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
from mesos.interface import mesos_pb2

from apache.aurora.executor.common.resource_manager import ResourceManager
from apache.thermos.config.schema import Resources
from apache.thermos.monitoring.process import ProcessSample
from apache.thermos.monitoring.resource import ResourceMonitorBase


def _mock_resource_monitor(num_procs=0, process_sample=ProcessSample.empty(), disk_usage=0):
  mock_resource_monitor = mock.Mock(spec=ResourceMonitorBase)
  mock_resource_monitor.sample.return_value = (
      12345,  # timestamp
      ResourceMonitorBase.AggregateResourceResult(num_procs, process_sample, disk_usage))

  return mock_resource_monitor


def test_resource_manager():
  resource_manager = ResourceManager(
      Resources(cpu=1, ram=128, disk=256),
      _mock_resource_monitor())

  assert resource_manager.status is None


def test_resource_manager_disk_exceeded():
  resources = Resources(cpu=1, ram=128, disk=256)
  resource_manager = ResourceManager(
      resources,
      _mock_resource_monitor(disk_usage=resources.disk().get() * 2))

  result = resource_manager.status
  assert result is not None
  assert result.reason.startswith('Disk limit exceeded')
  assert result.status == mesos_pb2.TASK_FAILED
