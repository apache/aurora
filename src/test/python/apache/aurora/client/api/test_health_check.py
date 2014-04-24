#
# Copyright 2013 Apache Software Foundation
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

from apache.aurora.common.http_signaler import HttpSignaler
from apache.aurora.client.api.health_check import (
  ChainedHealthCheck,
  HealthCheck,
  HealthStatus,
  HttpHealthCheck,
  NotRetriable,
  Retriable,
  InstanceWatcherHealthCheck,
  StatusHealthCheck
)

from gen.apache.aurora.api.ttypes import *

import mox
import pytest


PENDING = ScheduleStatus.PENDING
RUNNING = ScheduleStatus.RUNNING
FAILED = ScheduleStatus.FAILED


class HealthCheckTest(unittest.TestCase):
  HOST = 'host-a'
  HEALTH_PORT = 33333

  def setUp(self):
    self._http_signaler = mox.MockObject(HttpSignaler)
    self._status_health_check = StatusHealthCheck()
    self._http_health_check = HttpHealthCheck(self._http_signaler)
    self._smart_health_check = InstanceWatcherHealthCheck(self._http_signaler)
    self._health_check_a = mox.MockObject(HealthCheck)
    self._health_check_b = mox.MockObject(HealthCheck)

  def create_task(self, instance_id, task_id, status=RUNNING, host=HOST, port=HEALTH_PORT):
    ports = {}
    if port:
      ports['health'] = port
    assigned_task = AssignedTask(taskId=task_id,
                                 instanceId=instance_id,
                                 task=TaskConfig(),
                                 slaveHost=host,
                                 assignedPorts=ports)
    return ScheduledTask(assignedTask=assigned_task, status=status)

  def expect_http_signaler_creation(self, host=HOST, port=HEALTH_PORT):
    self._http_signaler(port, host).AndReturn(self._http_signaler)

  def expect_health_check(self, status=True):
    self._http_signaler.health().AndReturn((status, 'reason'))

  def replay(self):
    mox.Replay(self._http_signaler)
    mox.Replay(self._health_check_a)
    mox.Replay(self._health_check_b)

  def verify(self):
    mox.Verify(self._http_signaler)
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

  def test_http_health_check(self):
    """Verify successful and failed http health checks for a task"""
    task_a = self.create_task(0, 'a')
    self.expect_http_signaler_creation()
    self.expect_health_check()
    self.expect_health_check(status=False)
    self.replay()
    assert self._http_health_check.health(task_a) == Retriable.alive()
    assert self._http_health_check.health(task_a) == Retriable.dead()
    self.verify()

  def test_unmatched_host_port(self):
    """Test if an instance with a modified a (host, port) triggers a new http health checker creation"""
    instance_id = 0
    task_a = self.create_task(instance_id, 'a')
    self.expect_http_signaler_creation()
    self.expect_health_check()
    task_b = self.create_task(instance_id, 'b', host='host-b', port=44444)
    self.expect_http_signaler_creation(host='host-b', port=44444)
    self.expect_health_check()
    self.replay()
    assert self._http_health_check.health(task_a) == Retriable.alive()
    assert self._http_health_check.health(task_b) == Retriable.alive()
    self.verify()

  def test_simple_chained_health_check(self):
    """Verify successful health check"""
    task = self.create_task(0, 'a')
    self._health_check_a.health(task).AndReturn(Retriable.alive())
    self._health_check_b.health(task).AndReturn(Retriable.alive())
    health_check = ChainedHealthCheck(self._health_check_a, self._health_check_b)
    self.replay()
    assert health_check.health(task) == Retriable.alive()
    self.verify()

  def test_failed_primary_health_check(self):
    """Verifies that a secondary health check is not invoked when a primary check fails"""
    task = self.create_task(0, 'a')
    self._health_check_a.health(task).AndReturn(NotRetriable.dead())
    self._health_check_a.health(task).AndReturn(Retriable.dead())
    health_check = ChainedHealthCheck(self._health_check_a, self._health_check_b)
    self.replay()
    assert health_check.health(task) == NotRetriable.dead()
    assert health_check.health(task) == Retriable.dead()
    self.verify()

  def test_failed_secondary_health_check(self):
    """Verifies that a secondary health check is not invoked when a primary check fails"""
    task = self.create_task(0, 'a')
    self._health_check_a.health(task).AndReturn(Retriable.alive())
    self._health_check_b.health(task).AndReturn(Retriable.dead())
    health_check = ChainedHealthCheck(self._health_check_a, self._health_check_b)
    self.replay()
    assert health_check.health(task) == Retriable.dead()
    self.verify()

  def test_health_statuses(self):
    """Verfies that the health status tuple (health, retry_status) are as expected"""
    assert Retriable.alive() == (True, True)
    assert Retriable.dead() == (False, True)
    assert NotRetriable.alive() == (True, False)
    assert NotRetriable.dead() == (False, False)

  def test_instancewatcher_health_check(self):
    """Verifies that if the task has no health port, only status check is performed"""
    task = self.create_task(0, 'a', port=None)
    self.replay()
    assert self._smart_health_check.health(task) == Retriable.alive()
    self.verify()

  def test_instancewatcher_http_health_check(self):
    """Verifies that http health check is performed if the task has a health port"""
    task = self.create_task(0, 'a')
    self.expect_http_signaler_creation()
    self.expect_health_check(status=False)
    self.replay()
    assert self._smart_health_check.health(task) == Retriable.dead()
    self.verify()

  def test_instancewatcher_http_health_check_one_http_signaler(self):
    """Verifies that upon multiple http health checks only one HttpHealthChecker is created"""
    task = self.create_task(0, 'a')
    self.expect_http_signaler_creation()
    self.expect_health_check()
    self.expect_health_check()
    self.replay()
    assert self._smart_health_check.health(task) == Retriable.alive()
    assert self._smart_health_check.health(task) == Retriable.alive()
    self.verify()
