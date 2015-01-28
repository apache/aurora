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

import getpass
import os
import signal
import subprocess
import tempfile
import threading
import time
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict

from mesos.interface import mesos_pb2
from thrift.TSerialization import serialize
from twitter.common import log
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdtemp, safe_rmtree
from twitter.common.exceptions import ExceptionalThread
from twitter.common.log.options import LogOptions
from twitter.common.quantity import Amount, Time

from apache.aurora.config.schema.base import (
    HealthCheckConfig,
    MB,
    MesosJob,
    MesosTaskInstance,
    Process,
    Resources,
    Task
)
from apache.aurora.executor.aurora_executor import AuroraExecutor
from apache.aurora.executor.common.executor_timeout import ExecutorTimeout
from apache.aurora.executor.common.health_checker import HealthCheckerProvider
from apache.aurora.executor.common.sandbox import DirectorySandbox, SandboxProvider
from apache.aurora.executor.common.task_runner import TaskError
from apache.aurora.executor.status_manager import StatusManager
from apache.aurora.executor.thermos_task_runner import (
    DefaultThermosTaskRunnerProvider,
    ThermosTaskRunner
)
from apache.thermos.common.path import TaskPath
from apache.thermos.core.runner import TaskRunner
from apache.thermos.monitoring.monitor import TaskMonitor

from gen.apache.aurora.api.constants import AURORA_EXECUTOR_NAME
from gen.apache.aurora.api.ttypes import AssignedTask, ExecutorConfig, Identity, JobKey, TaskConfig

if 'THERMOS_DEBUG' in os.environ:
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('executor_logger')


class FastThermosExecutor(AuroraExecutor):
  STOP_WAIT = Amount(0, Time.SECONDS)


class FastStatusManager(StatusManager):
  POLL_WAIT = Amount(10, Time.MILLISECONDS)


class DefaultTestSandboxProvider(SandboxProvider):
  def from_assigned_task(self, assigned_task):
    return DirectorySandbox(safe_mkdtemp())


class FailingStartingTaskRunner(ThermosTaskRunner):
  def start(self):
    raise TaskError('I am an idiot!')


class FailingSandbox(DirectorySandbox):
  def create(self):
    raise self.CreationError('Could not create directory!')


class FailingSandboxProvider(SandboxProvider):
  def from_assigned_task(self, assigned_task):
    return FailingSandbox(safe_mkdtemp())


class SlowSandbox(DirectorySandbox):
  def __init__(self, *args, **kwargs):
    super(SlowSandbox, self).__init__(*args, **kwargs)
    self.is_initialized = lambda: False
    self._init_start = threading.Event()
    self._init_done = threading.Event()

  def create(self):
    self._init_start.wait()
    super(SlowSandbox, self).create()
    self.is_initialized = lambda: True
    self._init_done.set()


class SlowSandboxProvider(SandboxProvider):
  def from_assigned_task(self, assigned_task):
    return SlowSandbox(safe_mkdtemp())


class ProxyDriver(object):
  def __init__(self):
    self.method_calls = defaultdict(list)
    self._stop_event = threading.Event()

  def __getattr__(self, attr):
    def enqueue_arguments(*args, **kw):
      self.method_calls[attr].append((args, kw))
    return enqueue_arguments

  def stop(self, *args, **kw):
    self.method_calls['stop'].append((args, kw))
    self._stop_event.set()

  def wait_stopped(self):
    return self._stop_event.wait()


def make_task(thermos_config, assigned_ports={}, **kw):
  role = getpass.getuser()
  task_id = thermos_config.task().name().get() + '-001'
  at = AssignedTask(
      taskId=task_id,
      task=TaskConfig(
          executorConfig=ExecutorConfig(
              name=AURORA_EXECUTOR_NAME,
              data=thermos_config.json_dumps()),
          job=JobKey(role=role, environment='env', name='name'),
          owner=Identity(role=role, user=role)),
      assignedPorts=assigned_ports,
      **kw)
  td = mesos_pb2.TaskInfo()
  td.task_id.value = task_id
  td.name = thermos_config.task().name().get()
  td.data = serialize(at)
  return td


BASE_MTI = MesosTaskInstance(instance=0, role=getpass.getuser())
BASE_TASK = Task(resources=Resources(cpu=1.0, ram=16 * MB, disk=32 * MB))

HELLO_WORLD_TASK_ID = 'hello_world-001'
HELLO_WORLD = BASE_TASK(
    name='hello_world',
    processes=[Process(name='hello_world_{{thermos.task_id}}', cmdline='echo hello world')])
HELLO_WORLD_MTI = BASE_MTI(task=HELLO_WORLD)

SLEEP60 = BASE_TASK(processes=[Process(name='sleep60', cmdline='sleep 60')])
SLEEP2 = BASE_TASK(processes=[Process(name='sleep2', cmdline='sleep 2')])
SLEEP60_MTI = BASE_MTI(task=SLEEP60)

MESOS_JOB = MesosJob(
  name='does_not_matter',
  instances=1,
  role=getpass.getuser(),
)


def make_provider(checkpoint_root, runner_class=ThermosTaskRunner):
  return DefaultThermosTaskRunnerProvider(
      pex_location=os.path.join('dist', 'thermos_runner.pex'),
      checkpoint_root=checkpoint_root,
      task_runner_class=runner_class,
  )


def make_executor(
    proxy_driver,
    checkpoint_root,
    task,
    ports={},
    fast_status=False,
    runner_class=ThermosTaskRunner,
    status_providers=()):

  status_manager_class = FastStatusManager if fast_status else StatusManager
  runner_provider = make_provider(checkpoint_root, runner_class)
  te = FastThermosExecutor(
      runner_provider=runner_provider,
      status_manager_class=status_manager_class,
      sandbox_provider=DefaultTestSandboxProvider(),
      status_providers=status_providers,
  )

  ExecutorTimeout(te.launched, proxy_driver, timeout=Amount(100, Time.MILLISECONDS)).start()
  task_description = make_task(task, assigned_ports=ports, instanceId=0)
  te.launchTask(proxy_driver, task_description)

  te.status_manager_started.wait()
  sampled_metrics = te.metrics.sample()
  assert 'kill_manager.enabled' in sampled_metrics
  for checker in te._chained_checker._status_checkers:  # hacky
    assert ('%s.enabled' % checker.name()) in sampled_metrics

  while len(proxy_driver.method_calls['sendStatusUpdate']) < 2:
    time.sleep(0.1)

  # make sure startup was kosher
  updates = proxy_driver.method_calls['sendStatusUpdate']
  assert len(updates) == 2
  status_updates = [arg_tuple[0][0] for arg_tuple in updates]
  assert status_updates[0].state == mesos_pb2.TASK_STARTING
  assert status_updates[1].state == mesos_pb2.TASK_RUNNING

  # wait for the runner to bind to a task
  while True:
    runner = TaskRunner.get(task_description.task_id.value, checkpoint_root)
    if runner:
      break
    time.sleep(0.1)

  assert te.launched.is_set()
  return runner, te


class UnhealthyHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.end_headers()
    self.wfile.write('not ok')


class HealthyHandler(BaseHTTPRequestHandler):
  def do_GET(self):
    self.send_response(200)
    self.end_headers()
    self.wfile.write('ok')


class SignalServer(ExceptionalThread):
  def __init__(self, handler):
    self._server = HTTPServer(('', 0), handler)
    super(SignalServer, self).__init__()
    self.daemon = True
    self._stop = threading.Event()

  def run(self):
    while not self._stop.is_set():
      self._server.handle_request()

  def __enter__(self):
    self.start()
    return self._server.server_port

  def __exit__(self, exc_type, exc_val, traceback):
    self._stop.set()


class TestThermosExecutor(object):
  PANTS_BUILT = False
  LOG_DIR = None

  @classmethod
  def setup_class(cls):
    cls.LOG_DIR = tempfile.mkdtemp()
    LogOptions.set_log_dir(cls.LOG_DIR)
    LogOptions.set_disk_log_level('DEBUG')
    log.init('executor_logger')
    if not cls.PANTS_BUILT and 'SKIP_PANTS_BUILD' not in os.environ:
      assert subprocess.call(["./pants", "binary",
          "src/main/python/apache/thermos/bin:thermos_runner"]) == 0
      cls.PANTS_BUILT = True

  @classmethod
  def teardown_class(cls):
    if 'THERMOS_DEBUG' not in os.environ:
      safe_rmtree(cls.LOG_DIR)
    else:
      print('Saving executor logs in %s' % cls.LOG_DIR)

  def test_basic(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as tempdir:
      te = AuroraExecutor(
          runner_provider=make_provider(tempdir),
          sandbox_provider=DefaultTestSandboxProvider())
      te.launchTask(proxy_driver, make_task(HELLO_WORLD_MTI))
      te.terminated.wait()
      tm = TaskMonitor(TaskPath(root=tempdir), task_id=HELLO_WORLD_TASK_ID)
      runner_state = tm.get_state()

    assert 'hello_world_hello_world-001' in runner_state.processes, (
      'Could not find processes, got: %s' % ' '.join(runner_state.processes))

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    status_updates = [arg_tuple[0][0] for arg_tuple in updates]
    assert status_updates[0].state == mesos_pb2.TASK_STARTING
    assert status_updates[1].state == mesos_pb2.TASK_RUNNING
    assert status_updates[2].state == mesos_pb2.TASK_FINISHED

  def test_basic_as_job(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as tempdir:
      te = AuroraExecutor(
          runner_provider=make_provider(tempdir),
          sandbox_provider=DefaultTestSandboxProvider())
      te.launchTask(proxy_driver, make_task(MESOS_JOB(task=HELLO_WORLD), instanceId=0))
      te.runner_started.wait()
      while te._status_manager is None:
        time.sleep(0.1)
      te.terminated.wait()
      tm = TaskMonitor(TaskPath(root=tempdir), task_id=HELLO_WORLD_TASK_ID)
      runner_state = tm.get_state()

    assert 'hello_world_hello_world-001' in runner_state.processes, (
      'Could not find processes, got: %s' % ' '.join(runner_state.processes))
    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    status_updates = [arg_tuple[0][0] for arg_tuple in updates]
    assert status_updates[0].state == mesos_pb2.TASK_STARTING
    assert status_updates[1].state == mesos_pb2.TASK_RUNNING
    assert status_updates[2].state == mesos_pb2.TASK_FINISHED

  def test_runner_disappears(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      _, executor = make_executor(proxy_driver, checkpoint_root, SLEEP60_MTI, fast_status=True)
      while executor._runner is None or executor._runner._popen is None or (
          executor._runner._popen.pid is None):
        time.sleep(0.1)
      log.info('test_thermos_executor killing runner.pid %s' % executor._runner._popen.pid)
      os.kill(executor._runner._popen.pid, signal.SIGKILL)
      executor.terminated.wait()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb2.TASK_KILLED

  def test_task_killed(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      runner, executor = make_executor(proxy_driver, checkpoint_root, SLEEP60_MTI)
      runner.kill(force=True, preemption_wait=Amount(1, Time.SECONDS))
      executor.terminated.wait()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb2.TASK_KILLED

  def test_killTask(self):  # noqa
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      _, executor = make_executor(proxy_driver, checkpoint_root, SLEEP60_MTI)
      # send two, expect at most one delivered
      executor.killTask(proxy_driver, mesos_pb2.TaskID(value='sleep60-001'))
      executor.killTask(proxy_driver, mesos_pb2.TaskID(value='sleep60-001'))
      executor.terminated.wait()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb2.TASK_KILLED

  def test_shutdown(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      _, executor = make_executor(proxy_driver, checkpoint_root, SLEEP60_MTI)
      executor.shutdown(proxy_driver)
      executor.terminated.wait()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb2.TASK_KILLED

  def test_task_health_failed(self):
    proxy_driver = ProxyDriver()
    with SignalServer(UnhealthyHandler) as port:
      with temporary_dir() as checkpoint_root:
        health_check_config = HealthCheckConfig(initial_interval_secs=0.1, interval_secs=0.1)
        _, executor = make_executor(
            proxy_driver,
            checkpoint_root,
            MESOS_JOB(task=SLEEP60, health_check_config=health_check_config),
            ports={'health': port},
            fast_status=True,
            status_providers=(HealthCheckerProvider(),))
        executor.terminated.wait()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb2.TASK_FAILED

  def test_task_health_ok(self):
    proxy_driver = ProxyDriver()
    with SignalServer(HealthyHandler) as port:
      with temporary_dir() as checkpoint_root:
        health_check_config = HealthCheckConfig(initial_interval_secs=0.1, interval_secs=0.1)
        _, executor = make_executor(proxy_driver,
                                    checkpoint_root,
                                    MESOS_JOB(task=SLEEP2, health_check_config=health_check_config),
                                    ports={'health': port},
                                    fast_status=True,
                                    status_providers=(HealthCheckerProvider(),))
        executor.terminated.wait()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb2.TASK_FINISHED

  def test_failing_runner_start(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as td:
      runner_provider = make_provider(td, FailingStartingTaskRunner)
      te = FastThermosExecutor(
          runner_provider=runner_provider,
          sandbox_provider=DefaultTestSandboxProvider())
      te.launchTask(proxy_driver, make_task(HELLO_WORLD_MTI))
      proxy_driver.wait_stopped()

      updates = proxy_driver.method_calls['sendStatusUpdate']
      assert updates[-1][0][0].state == mesos_pb2.TASK_FAILED

  def test_failing_runner_initialize(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as td:
      te = FastThermosExecutor(
          runner_provider=make_provider(td),
          sandbox_provider=FailingSandboxProvider())
      te.launchTask(proxy_driver, make_task(HELLO_WORLD_MTI))
      proxy_driver.wait_stopped()

      updates = proxy_driver.method_calls['sendStatusUpdate']
      assert updates[-1][0][0].state == mesos_pb2.TASK_FAILED

  def test_slow_runner_initialize(self):
    proxy_driver = ProxyDriver()

    task = make_task(HELLO_WORLD_MTI)

    with temporary_dir() as td:
      te = FastThermosExecutor(
          runner_provider=make_provider(td),
          sandbox_provider=SlowSandboxProvider())
      te.SANDBOX_INITIALIZATION_TIMEOUT = Amount(1, Time.MILLISECONDS)
      te.launchTask(proxy_driver, task)
      proxy_driver.wait_stopped()

      updates = proxy_driver.method_calls['sendStatusUpdate']
      assert len(updates) == 2
      assert updates[-1][0][0].state == mesos_pb2.TASK_FAILED

      te._sandbox._init_start.set()

  def test_killTask_during_runner_initialize(self):  # noqa
    proxy_driver = ProxyDriver()

    task = make_task(HELLO_WORLD_MTI)

    with temporary_dir() as td:
      te = FastThermosExecutor(
          runner_provider=make_provider(td),
          sandbox_provider=SlowSandboxProvider())
      te.launchTask(proxy_driver, task)
      te.sandbox_initialized.wait()
      te.killTask(proxy_driver, mesos_pb2.TaskID(value=task.task_id.value))
      assert te.runner_aborted.is_set()
      assert not te.sandbox_created.is_set()

      # we've simulated a "slow" initialization by blocking it until the killTask was sent - so now,
      # trigger the initialization to complete
      te._sandbox._init_start.set()

      # however, wait on the runner to definitely finish its initialization before continuing
      # (otherwise, this function races ahead too fast)
      te._sandbox._init_done.wait()
      te.sandbox_created.wait()
      assert te.sandbox_initialized.is_set()
      assert te.sandbox_created.is_set()

      proxy_driver.wait_stopped()

      updates = proxy_driver.method_calls['sendStatusUpdate']
      assert len(updates) == 2
      assert updates[-1][0][0].state == mesos_pb2.TASK_KILLED

  def test_launchTask_deserialization_fail(self):  # noqa
    proxy_driver = ProxyDriver()

    role = getpass.getuser()
    task_info = mesos_pb2.TaskInfo()
    task_info.name = task_info.task_id.value = 'broken'
    task_info.data = serialize(
        AssignedTask(
            task=TaskConfig(
                job=JobKey(role=role, environment='env', name='name'),
                owner=Identity(role=role, user=role),
                executorConfig=ExecutorConfig(name=AURORA_EXECUTOR_NAME, data='garbage'))))

    te = FastThermosExecutor(
        runner_provider=make_provider(safe_mkdtemp()),
        sandbox_provider=DefaultTestSandboxProvider())
    te.launchTask(proxy_driver, task_info)
    proxy_driver.wait_stopped()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 2
    assert updates[0][0][0].state == mesos_pb2.TASK_STARTING
    assert updates[1][0][0].state == mesos_pb2.TASK_FAILED


def test_waiting_executor():
  proxy_driver = ProxyDriver()
  with temporary_dir() as checkpoint_root:
    te = AuroraExecutor(
        runner_provider=make_provider(checkpoint_root),
        sandbox_provider=DefaultTestSandboxProvider())
    ExecutorTimeout(te.launched, proxy_driver, timeout=Amount(100, Time.MILLISECONDS)).start()
    proxy_driver.wait_stopped()
