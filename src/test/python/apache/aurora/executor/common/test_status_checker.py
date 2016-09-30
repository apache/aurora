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

import threading

from mesos.interface import mesos_pb2

from apache.aurora.executor.common.status_checker import (
    ChainedStatusChecker,
    Healthy,
    StatusChecker,
    StatusResult
)


class EventHealth(StatusChecker):
  def __init__(self):
    self.started = threading.Event()
    self.stopped = threading.Event()
    self._status = None

  @property
  def status(self):
    return self._status

  def set_status(self, status):
    self._status = status

  def start(self):
    self.started.set()

  def stop(self):
    self.stopped.set()


def test_chained_health_interface():
  hi = ChainedStatusChecker([])
  assert hi.status is None

  hi = ChainedStatusChecker([Healthy()])
  assert hi.status is None

  si1 = EventHealth()
  si2 = EventHealth()
  chained_si = ChainedStatusChecker([si1, si2])

  for si in (si1, si2):
    assert not si.started.is_set()
  chained_si.start()
  for si in (si1, si2):
    assert si.started.is_set()

  assert chained_si.status is None
  reason = StatusResult('derp', mesos_pb2.TASK_FAILED)
  si2.set_status(reason)
  assert chained_si.status == reason
  assert chained_si.status.reason == 'derp'
  assert mesos_pb2.TaskState.Name(chained_si.status.status) == 'TASK_FAILED'

  for si in (si1, si2):
    assert not si.stopped.is_set()
  chained_si.stop()
  for si in (si1, si2):
    assert si.stopped.is_set()

  # A task may fail after transition into RUNNING state. We need to test
  # the status is not memoized in ChainedStatusChecker.
  si1 = EventHealth()
  si2 = EventHealth()
  chained_si = ChainedStatusChecker([si1, si2])

  for si in (si1, si2):
    assert not si.started.is_set()
  chained_si.start()
  for si in (si1, si2):
    assert si.started.is_set()

  assert chained_si.status is None
  reason2 = StatusResult('Task is healthy.', mesos_pb2.TASK_RUNNING)
  si2.set_status(reason2)
  assert chained_si.status == reason2
  assert chained_si.status.reason == 'Task is healthy.'
  assert mesos_pb2.TaskState.Name(chained_si.status.status) == 'TASK_RUNNING'

  reason1 = StatusResult('derp', mesos_pb2.TASK_FAILED)
  si1.set_status(reason1)
  assert chained_si.status == reason1
  assert chained_si.status.reason == 'derp'
  assert mesos_pb2.TaskState.Name(chained_si.status.status) == 'TASK_FAILED'

  for si in (si1, si2):
    assert not si.stopped.is_set()
  chained_si.stop()
  for si in (si1, si2):
    assert si.stopped.is_set()
