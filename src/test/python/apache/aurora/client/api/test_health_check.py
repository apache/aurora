#
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

import mox
import pytest

from apache.aurora.client.api.health_check import (
    HealthCheck,
    NotRetriable,
    Retriable,
    StatusHealthCheck
)

from gen.apache.aurora.api.ttypes import AssignedTask, ScheduledTask, ScheduleStatus, TaskConfig

PENDING = ScheduleStatus.PENDING
RUNNING = ScheduleStatus.RUNNING
FAILED = ScheduleStatus.FAILED


class HealthCheckTest(unittest.TestCase):

  def setUp(self):
    self._status_health_check = StatusHealthCheck()
    self._health_check_a = mox.MockObject(HealthCheck)
    self._health_check_b = mox.MockObject(HealthCheck)

  def create_task(self, instance_id, task_id, status=RUNNING):
    assigned_task = AssignedTask(taskId=task_id,
                                 instanceId=instance_id,
                                 task=TaskConfig())
    return ScheduledTask(assignedTask=assigned_task, status=status)

  def replay(self):
    mox.Replay(self._health_check_a)
    mox.Replay(self._health_check_b)

  def verify(self):
    mox.Verify(self._health_check_a)
    mox.Verify(self._health_check_b)

  def test_simple_status_health_check(self):
    """Verify that running instances are reported healthy"""
    task_a = self.create_task(0, 'a')
    task_b = self.create_task(1, 'b')
    assert self._status_health_check.health(task_a) == Retriable.alive()
    assert self._status_health_check.health(task_b) == Retriable.alive()

  def test_failed_status_health_check(self):
    """Verify that the health check fails for tasks in a state other than RUNNING"""
    pending_task = self.create_task(0, 'a', status=PENDING)
    failed_task = self.create_task(1, 'b', status=FAILED)
    assert self._status_health_check.health(pending_task) == Retriable.dead()
    assert self._status_health_check.health(failed_task) == Retriable.dead()

  def test_changed_task_id(self):
    """Verifes that an instance with a different task id causes the health check to fail"""
    task_a = self.create_task(0, 'a')
    task_b = self.create_task(0, 'b')
    assert self._status_health_check.health(task_a) == Retriable.alive()
    assert self._status_health_check.health(task_b) == NotRetriable.dead()

  def test_health_statuses(self):
    """Verfies that the health status tuple (health, retry_status) are as expected"""
    assert Retriable.alive() == (True, True)
    assert Retriable.dead() == (False, True)
    assert NotRetriable.alive() == (True, False)
    assert NotRetriable.dead() == (False, False)
