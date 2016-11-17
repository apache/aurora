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

import pytest
from mesos.interface.mesos_pb2 import TaskState

from apache.aurora.executor.common.status_checker import (
    ChainedStatusChecker,
    Healthy,
    StatusChecker,
    StatusResult
)

TASK_STARTING = StatusResult(None, TaskState.Value('TASK_STARTING'))
TASK_RUNNING = StatusResult(None, TaskState.Value('TASK_RUNNING'))
TASK_FAILED = StatusResult(None, TaskState.Value('TASK_FAILED'))


class EventHealth(StatusChecker):
  def __init__(self, status=None):
    self.started = threading.Event()
    self.stopped = threading.Event()
    self._status = status

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
  reason = StatusResult('derp', TaskState.Value('TASK_FAILED'))
  si2.set_status(reason)
  assert chained_si.status == reason
  assert chained_si.status.reason == 'derp'
  assert TaskState.Name(chained_si.status.status) == 'TASK_FAILED'

  for si in (si1, si2):
    assert not si.stopped.is_set()
  chained_si.stop()
  for si in (si1, si2):
    assert si.stopped.is_set()


def test_chained_empty_checkers():
  hi = ChainedStatusChecker([])
  assert hi.status is None


def test_chained_healthy_status_none():
  hi = ChainedStatusChecker([EventHealth()])
  assert hi.status is None

  hi = ChainedStatusChecker([EventHealth(), EventHealth(), EventHealth()])
  assert hi.status is None


def test_chained_healthy_status_starting():
  hi = ChainedStatusChecker([EventHealth(TASK_STARTING)])
  assert hi.status is TASK_STARTING

  hi = ChainedStatusChecker([EventHealth(TASK_STARTING),
      EventHealth(TASK_STARTING),
      EventHealth(TASK_STARTING)])
  assert hi.status is TASK_STARTING


def test_chained_healthy_status_running():
  hi = ChainedStatusChecker([EventHealth(TASK_RUNNING)])
  assert hi.status is TASK_RUNNING

  hi = ChainedStatusChecker([EventHealth(TASK_RUNNING),
      EventHealth(TASK_RUNNING),
      EventHealth(TASK_RUNNING)])
  assert hi.status is TASK_RUNNING


def test_chained_healthy_status_failed():
  hi = ChainedStatusChecker([EventHealth(TASK_FAILED)])
  assert hi.status is TASK_FAILED

  hi = ChainedStatusChecker([EventHealth(TASK_FAILED),
      EventHealth(TASK_FAILED),
      EventHealth(TASK_FAILED)])
  assert hi.status is TASK_FAILED


def test_chained_status_failed_trumps_all():
  hi = ChainedStatusChecker([EventHealth(),
      EventHealth(TASK_RUNNING),
      EventHealth(TASK_STARTING),
      EventHealth(TASK_FAILED)])
  assert hi.status is TASK_FAILED

  hi = ChainedStatusChecker([EventHealth(TASK_FAILED),
      EventHealth(TASK_STARTING),
      EventHealth(TASK_RUNNING),
      EventHealth()])
  assert hi.status is TASK_FAILED


def test_chained_status_starting_trumps_running_and_none():
  hi = ChainedStatusChecker([EventHealth(), EventHealth(TASK_RUNNING), EventHealth(TASK_STARTING)])
  assert hi.status is TASK_STARTING

  hi = ChainedStatusChecker([EventHealth(TASK_STARTING), EventHealth(TASK_RUNNING), EventHealth()])
  assert hi.status is TASK_STARTING


def test_chained_status_running_trumps_none():
  hi = ChainedStatusChecker([EventHealth(TASK_RUNNING), EventHealth()])
  assert hi.status is TASK_RUNNING

  hi = ChainedStatusChecker([EventHealth(), EventHealth(TASK_RUNNING)])
  assert hi.status is TASK_RUNNING


def test_chained_status_starting_to_running_consensus():
  eh1 = EventHealth(TASK_STARTING)
  eh2 = EventHealth(TASK_STARTING)
  hi = ChainedStatusChecker([eh1, eh2])
  assert hi.status is TASK_STARTING

  eh1.set_status(TASK_RUNNING)
  assert hi.status is TASK_STARTING

  eh2.set_status(TASK_RUNNING)
  assert hi.status is TASK_RUNNING


def test_chained_status_failed_is_terminal():
  eh = EventHealth(TASK_FAILED)
  hi = ChainedStatusChecker([eh])
  assert hi.status is TASK_FAILED

  eh.set_status(TASK_RUNNING)
  assert hi.status is TASK_FAILED

  eh.set_status(TASK_STARTING)
  assert hi.status is TASK_FAILED

  eh.set_status(None)
  assert hi.status is TASK_FAILED


def test_chained_status_raises_unknown_status_result():
  hi = ChainedStatusChecker([EventHealth(1)])
  with pytest.raises(TypeError):
    hi.status
