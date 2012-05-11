from collections import defaultdict
import functools
import getpass
import json
import os
import subprocess
import tempfile
import time

from thrift.TSerialization import serialize
import mesos_pb2 as mesos_pb
from gen.twitter.mesos.ttypes import (
  AssignedTask,
  TwitterTaskInfo)
from gen.twitter.thermos.ttypes import TaskState

from twitter.common import log
from twitter.common.log.options import LogOptions
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.quantity import Amount, Time, Data
from twitter.mesos.config.schema import (
  MesosTaskInstance,
  Task,
  Process,
  Resources)
from twitter.mesos.executor.thermos_executor import ThermosExecutor
from twitter.mesos.executor.task_runner_wrapper import TaskRunnerWrapper
from twitter.mesos.executor.sandbox_manager import DirectorySandbox
from twitter.thermos.runner.runner import TaskRunner
from twitter.thermos.base.path import TaskPath


class TestTaskRunner(TaskRunnerWrapper):
  def __init__(self, task_id, *args, **kwargs):
    self._tempdir = tempfile.mkdtemp()
    self._runner_pex = os.path.join('dist', 'thermos_run.pex')
    self._sandbox = DirectorySandbox(task_id, sandbox_root=self._tempdir)
    self._enable_chroot = False
    TaskRunnerWrapper.__init__(self, task_id, *args, **kwargs)


class ProxyDriver(object):
  def __init__(self):
    self.method_calls = defaultdict(list)

  def __getattr__(self, attr):
    def enqueue_arguments(*args, **kw):
      self.method_calls[attr].append((args, kw))
    return enqueue_arguments


def make_task(thermos_config, assigned_ports={}):
  at = AssignedTask(task = TwitterTaskInfo(thermosConfig = json.dumps(thermos_config.get())),
                    assignedPorts = assigned_ports)
  td = mesos_pb.TaskDescription()
  td.task_id.value = thermos_config.task().name().get() + '-001'
  td.name = thermos_config.task().name().get()
  td.data = serialize(at)
  return td


def hello_world():
  return MesosTaskInstance(
    task = Task(name = 'hello_world',
                processes = [
                  Process(name = 'hello_world', cmdline = 'echo hello world')
                ],
                resources = Resources(cpu=1.0, ram=1024, disk=1024)),
    instance = 0,
    role = getpass.getuser())


def sleep60():
  return MesosTaskInstance(
    task = Task(name = 'sleep60',
                processes = [
                  Process(name = 'sleep60', cmdline = 'sleep 60')
                ],
                resources = Resources(cpu=1.0, ram=1024, disk=1024)),
    instance = 0,
    role = getpass.getuser())


class TestThermosExecutor(object):
  PANTS_BUILT = False
  LOG_DIR = None

  @classmethod
  def setup_class(cls):
    cls.LOG_DIR = tempfile.mkdtemp()
    LogOptions.set_log_dir(cls.LOG_DIR)
    log.init('executor_logger')
    if not TestThermosExecutor.PANTS_BUILT:
      assert subprocess.call(["./pants", "src/python/twitter/thermos/bin:thermos_run"]) == 0
      PANTS_BUILD = True

  @classmethod
  def teardown_class(cls):
    safe_rmtree(cls.LOG_DIR)

  def test_basic(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as tempdir:
      runner_class = functools.partial(TestTaskRunner, checkpoint_root=tempdir)
      te = ThermosExecutor(runner_class=runner_class)
      te.launchTask(proxy_driver, make_task(hello_world()))
      while te._runner.is_alive():
        time.sleep(0.1)
      te._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    status_updates = [arg_tuple[0][0] for arg_tuple in updates]
    assert status_updates[0].state == mesos_pb.TASK_STARTING
    assert status_updates[1].state == mesos_pb.TASK_RUNNING
    assert status_updates[2].state == mesos_pb.TASK_FINISHED

  def test_task_killed(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      runner_class = functools.partial(TestTaskRunner, checkpoint_root=checkpoint_root)
      te = ThermosExecutor(runner_class=runner_class)
      task_description = make_task(sleep60())
      te.launchTask(proxy_driver, task_description)
      while not te._runner.is_started():
        time.sleep(0.1)
      while te._runner.task_state() != TaskState.ACTIVE:
        time.sleep(0.1)

      # This is a bug, ugh.
      task_json = TaskPath(root = checkpoint_root, task_id = task_description.task_id.value,
                           state = 'active').getpath('task_path')
      while not os.path.exists(task_json):
        time.sleep(0.1)

      # make sure startup was kosher
      updates = proxy_driver.method_calls['sendStatusUpdate']
      assert len(updates) == 2
      status_updates = [arg_tuple[0][0] for arg_tuple in updates]
      assert status_updates[0].state == mesos_pb.TASK_STARTING
      assert status_updates[1].state == mesos_pb.TASK_RUNNING

      # kill the existing runner
      runner = TaskRunner.get(task_description.task_id.value, checkpoint_root)
      assert runner is not None
      runner.kill(force=True)

      te._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_KILLED

  # TODO(wickman) This is mostly copy&paste of the previous test.  Do pytest
  # parameterization here if possible.
  def test_task_lost(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      runner_class = functools.partial(TestTaskRunner, checkpoint_root=checkpoint_root)
      te = ThermosExecutor(runner_class=runner_class)
      task_description = make_task(sleep60())
      te.launchTask(proxy_driver, task_description)
      while not te._runner.is_started():
        time.sleep(0.1)
      while te._runner.task_state() != TaskState.ACTIVE:
        time.sleep(0.1)

      task_json = TaskPath(root = checkpoint_root, task_id = task_description.task_id.value,
                           state = 'active').getpath('task_path')
      while not os.path.exists(task_json):
        time.sleep(0.1)

      # make sure startup was kosher
      updates = proxy_driver.method_calls['sendStatusUpdate']
      assert len(updates) == 2
      status_updates = [arg_tuple[0][0] for arg_tuple in updates]
      assert status_updates[0].state == mesos_pb.TASK_STARTING
      assert status_updates[1].state == mesos_pb.TASK_RUNNING

      # lose the existing runner
      runner = TaskRunner.get(task_description.task_id.value, checkpoint_root)
      assert runner is not None
      runner.lose(force=True)

      te._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_LOST
