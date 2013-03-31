import unittest

from twitter.mesos.common.http_signaler import HttpSignaler
from twitter.mesos.client.health_check_factory import (
  ChainedHealthCheck,
  HealthCheck,
  HealthCheckFactory,
  HttpHealthCheck,
  StatusHealthCheck
)

from gen.twitter.mesos.ttypes import *

import mox
import pytest

PENDING = ScheduleStatus.PENDING
RUNNING = ScheduleStatus.RUNNING
FAILED = ScheduleStatus.FAILED

class HealthCheckFactoryTest(unittest.TestCase):
  HOST = 'host-a'
  HEALTH_PORT = 33333

  def setUp(self):
    self._http_signaler = mox.MockObject(HttpSignaler)
    self._status_health_check = StatusHealthCheck()
    self._http_health_check = HttpHealthCheck(self._http_signaler)
    self._health_check_a = mox.MockObject(HealthCheck)
    self._health_check_b = mox.MockObject(HealthCheck)

  def create_task(self, shard_id, task_id, status=RUNNING, host=HOST, port=HEALTH_PORT):
    task = TwitterTaskInfo(shardId=shard_id)
    ports = {'health': port}
    assigned_task = AssignedTask(taskId=task_id, task=task, slaveHost=host, assignedPorts=ports)
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
    """Verify that running shards are reported healthy"""
    task_a = self.create_task(0, 'a')
    task_b = self.create_task(1, 'b')
    assert self._status_health_check.health(task_a)
    assert self._status_health_check.health(task_b)

  def test_failed_status_health_check(self):
    """Verify that the health check fails for tasks in a state other than RUNNING"""
    pending_task = self.create_task(0, 'a', status=PENDING)
    failed_task = self.create_task(1, 'b', status=FAILED)
    assert not self._status_health_check.health(pending_task)
    assert not self._status_health_check.health(failed_task)

  def test_changed_task_id(self):
    """Verifes that a shard with a different task id causes the health check to fail"""
    task_a = self.create_task(0, 'a')
    task_b = self.create_task(0, 'b')
    assert self._status_health_check.health(task_a)
    assert not self._status_health_check.health(task_b)

  def test_http_health_check(self):
    """Verify successful and failed http health checks for a task"""
    task_a = self.create_task(0, 'a')
    self.expect_http_signaler_creation()
    self.expect_health_check()
    self.expect_health_check(status=False)
    self.replay()
    assert self._http_health_check.health(task_a)
    assert not self._http_health_check.health(task_a)
    self.verify()

  def test_unmatched_host_port(self):
    """Test if a shard with a modified a (host, port) triggers a new http health checker creation"""
    shard_id = 0
    task_a = self.create_task(shard_id, 'a')
    self.expect_http_signaler_creation()
    self.expect_health_check()
    task_b = self.create_task(shard_id, 'b', host='host-b', port=44444)
    self.expect_http_signaler_creation(host='host-b', port=44444)
    self.expect_health_check()
    self.replay()
    assert self._http_health_check.health(task_a)
    assert self._http_health_check.health(task_b)
    self.verify()

  def test_simple_chanined_health_check(self):
    """Verify successful health check"""
    task = self.create_task(0, 'a')
    self._health_check_a.health(task).AndReturn(True)
    self._health_check_b.health(task).AndReturn(True)
    health_check = ChainedHealthCheck(self._health_check_a, self._health_check_b)
    self.replay()
    assert health_check.health(task)
    self.verify()

  def test_failed_primary_health_check(self):
    """Verifies that a secondary health check is not invoked when a primary check fails"""
    task = self.create_task(0, 'a')
    self._health_check_a.health(task).AndReturn(False)
    health_check = ChainedHealthCheck(self._health_check_a, self._health_check_b)
    self.replay()
    assert not health_check.health(task)
    self.verify()
