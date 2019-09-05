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
import pytest
from twitter.common.quantity import Amount, Time

from apache.thermos.monitoring.disk import DuDiskCollector, MesosDiskCollector
from apache.thermos.monitoring.monitor import TaskMonitor
from apache.thermos.monitoring.process import ProcessSample
from apache.thermos.monitoring.resource import (
    DiskCollectorProvider,
    HistoryProvider,
    NullTaskResourceMonitor,
    ResourceHistory,
    ResourceMonitorBase,
    TaskResourceMonitor
)

from gen.apache.thermos.ttypes import ProcessStatus


class TestResourceHistoryProvider(TestCase):
  def test_too_long_history(self):
    with pytest.raises(ValueError):
      HistoryProvider().provides(Amount(1, Time.DAYS), 1)


class TestDiskCollectorProvider(TestCase):
  def test_default_collector_class(self):
    assert isinstance(DiskCollectorProvider().provides("some_path"), DuDiskCollector)

  def test_mesos_collector_class(self):
    assert isinstance(
      DiskCollectorProvider(enable_mesos_disk_collector=True).provides("some_path"),
      MesosDiskCollector)


class TestResourceHistory(TestCase):
  def setUp(self):
    self.max_len = 4
    self.resource_history = ResourceHistory(self.max_len)

  def test_add(self):
    next_resource_stamp = time() + 100
    value = ResourceMonitorBase.FullResourceResult({}, 0)

    assert (next_resource_stamp, value) not in self.resource_history._values
    self.resource_history.add(next_resource_stamp, value)
    assert (next_resource_stamp, value) == self.resource_history._values[1]

  def test_add_prevents_old_entries(self):
    with self.assertRaises(ValueError):
      self.resource_history.add(-1, 10)

  def test_get(self):
    resource_stamp = time() + 100

    value = ResourceMonitorBase.FullResourceResult({}, 0)
    value_wrong = ResourceMonitorBase.FullResourceResult({}, 50)

    self.resource_history.add(resource_stamp, value)
    self.resource_history.add(resource_stamp + 1000, value_wrong)
    self.resource_history.add(resource_stamp + 10000, value_wrong)
    assert resource_stamp, value == self.resource_history.get(resource_stamp)


class TestTaskResourceMonitor(TestCase):
  class FakeResourceHistoryProvider(object):
    def __init__(self, history):
      self.history = history

    def provides(self, history_time, min_collection_interval):
      return self.history

  @mock.patch('apache.thermos.monitoring.process_collector_psutil.ProcessTreeCollector.sample',
      autospec=True, spec_set=True)
  @mock.patch('apache.thermos.monitoring.monitor.TaskMonitor.get_active_processes',
      autospec=True, spec_set=True)
  def test_sample_by_process_without_history(self, mock_get_active_processes, mock_sample):
    fake_process_name = 'fake-process-name'
    task_path = '.'
    task_monitor = TaskMonitor(task_path, 'fake-task-id')
    fake_process_status = ProcessStatus(process=fake_process_name)
    mock_get_active_processes.return_value = [(fake_process_status, 1)]
    fake_process_sample = ProcessSample.empty()
    mock_sample.return_value = fake_process_sample

    task_resource_monitor = TaskResourceMonitor('fake-task-id', task_monitor)
    assert task_resource_monitor.name == 'TaskResourceMonitor[fake-task-id]'

    assert fake_process_sample == task_resource_monitor.sample_by_process(fake_process_name)
    assert mock_get_active_processes.mock_calls == [mock.call(task_monitor)]
    assert mock_sample.mock_calls == [mock.call(
        task_resource_monitor._process_collectors[fake_process_status])]

  @mock.patch('apache.thermos.monitoring.monitor.TaskMonitor.get_active_processes',
      autospec=True, spec_set=True)
  def test_sample_by_process_from_history(self, mock_get_active_processes):

    fake_process_name_1 = 'fake-process-name-1'
    fake_process_name_2 = 'fake-process-name-2'
    task_path = '.'
    task_monitor = TaskMonitor(task_path, 'fake-task-id')
    fake_process_status_1 = ProcessStatus(process=fake_process_name_1)
    fake_process_status_2 = ProcessStatus(process=fake_process_name_2)
    mock_get_active_processes.return_value = [(fake_process_status_1, 1),
                                              (fake_process_status_2, 2)]

    fake_history = ResourceHistory(2)
    fake_history.add(time(), ResourceMonitorBase.FullResourceResult(
        {fake_process_status_1: ResourceMonitorBase.ProcResourceResult(ProcessSample.empty(), 1),
         fake_process_status_2: ResourceMonitorBase.ProcResourceResult(ProcessSample.empty(), 2),
         }, 10))

    task_resource_monitor = TaskResourceMonitor('fake-task-id', task_monitor,
        history_provider=self.FakeResourceHistoryProvider(fake_history))

    assert task_resource_monitor.name == 'TaskResourceMonitor[fake-task-id]'
    assert task_resource_monitor.sample_by_process(fake_process_name_1) == ProcessSample.empty()
    assert task_resource_monitor.sample_by_process(fake_process_name_2) == ProcessSample.empty()

    _, sample = task_resource_monitor.sample()
    assert sample.num_procs == 3  # 1 pid in fake_process_status_1 and 2 in fake_process_status_2
    assert sample.process_sample == ProcessSample.empty()
    assert sample.disk_usage == 10
    assert mock_get_active_processes.mock_calls == [mock.call(task_monitor),
        mock.call(task_monitor)]

  @mock.patch('apache.thermos.monitoring.monitor.TaskMonitor.get_active_processes',
      autospec=True, spec_set=True)
  def test_sample_by_process_no_process(self, mock_get_active_processes):
    task_path = '.'

    task_monitor = TaskMonitor(task_path, 'fake-task-id')
    mock_get_active_processes.return_value = []

    task_resource_monitor = TaskResourceMonitor('fake-task-id', task_monitor)

    with self.assertRaises(ValueError):
      task_resource_monitor.sample_by_process('fake-process-name')

    assert mock_get_active_processes.mock_calls == [mock.call(task_monitor)]


class TestNullTaskResourceMonitor(TestCase):
  def test_null_sample(self):
    monitor = NullTaskResourceMonitor()
    monitor.start()

    null_aggregate = (0, ProcessSample.empty(), 0)

    assert monitor.sample()[1] == null_aggregate
    assert monitor.sample_at(time())[1] == null_aggregate
    assert monitor.sample_by_process("any_process") == ProcessSample.empty()

    monitor.kill()
