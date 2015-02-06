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

from time import time
from unittest import TestCase

import mock

from apache.thermos.common.path import TaskPath
from apache.thermos.monitoring.monitor import TaskMonitor
from apache.thermos.monitoring.process import ProcessSample
from apache.thermos.monitoring.resource import (
    ResourceHistory,
    ResourceMonitorBase,
    TaskResourceMonitor
)

from gen.apache.thermos.ttypes import ProcessStatus


class TestResourceHistory(TestCase):
  def setUp(self):
    self.max_len = 4
    self.resource_history = ResourceHistory(self.max_len)

  def test_add(self):
    next_resource_stamp = time() + 100
    value = ResourceMonitorBase.ResourceResult(1, 1, 0)

    assert (next_resource_stamp, value) not in self.resource_history._values
    self.resource_history.add(next_resource_stamp, value)
    assert (next_resource_stamp, value) == self.resource_history._values[1]

  def test_add_prevents_old_entries(self):
    with self.assertRaises(ValueError):
      self.resource_history.add(-1, 10)

  def test_get(self):
    resource_stamp = time() + 100
    value = ResourceMonitorBase.ResourceResult(1, 1, 0)
    value_wrong = ResourceMonitorBase.ResourceResult(1, 1, 50)

    self.resource_history.add(resource_stamp, value)
    self.resource_history.add(resource_stamp + 1000, value_wrong)
    self.resource_history.add(resource_stamp + 10000, value_wrong)
    assert resource_stamp, value == self.resource_history.get(resource_stamp)


class TestTaskResouceMonitor(TestCase):
  @mock.patch('apache.thermos.monitoring.process_collector_psutil.ProcessTreeCollector.sample',
      autospec=True, spec_set=True)
  @mock.patch('apache.thermos.monitoring.monitor.TaskMonitor.get_active_processes',
      autospec=True, spec_set=True)
  def test_sample_by_process(self, mock_get_active_processes, mock_sample):
    fake_process_name = 'fake-process-name'
    task_path = TaskPath(root='.')
    task_monitor = TaskMonitor(task_path, 'fake-task-id')
    fake_process_status = ProcessStatus(process=fake_process_name)
    mock_get_active_processes.return_value = [(fake_process_status, 1)]
    fake_process_sample = ProcessSample.empty()
    mock_sample.return_value = fake_process_sample

    task_resource_monitor = TaskResourceMonitor(task_monitor, '.')

    assert fake_process_sample == task_resource_monitor.sample_by_process(fake_process_name)
    assert mock_get_active_processes.mock_calls == [mock.call(task_monitor)]
    assert mock_sample.mock_calls == [mock.call(
        task_resource_monitor._process_collectors[fake_process_status])]

  @mock.patch('apache.thermos.monitoring.monitor.TaskMonitor.get_active_processes',
      autospec=True, spec_set=True)
  def test_sample_by_process_no_process(self, mock_get_active_processes):
    task_path = TaskPath(root='.')

    task_monitor = TaskMonitor(task_path, 'fake-task-id')
    mock_get_active_processes.return_value = []

    task_resource_monitor = TaskResourceMonitor(task_monitor, '.')

    with self.assertRaises(ValueError):
      task_resource_monitor.sample_by_process('fake-process-name')

    assert mock_get_active_processes.mock_calls == [mock.call(task_monitor)]
