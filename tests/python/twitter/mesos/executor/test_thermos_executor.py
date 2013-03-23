from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from collections import defaultdict
import functools
import getpass
import json
import os
import signal
import subprocess
import tempfile
import threading
import time

from thrift.TSerialization import serialize
import mesos_pb2 as mesos_pb
from gen.twitter.mesos.ttypes import (
  AssignedTask,
  TwitterTaskInfo)
from gen.twitter.thermos.ttypes import TaskState

from twitter.common import log
from twitter.common.exceptions import ExceptionalThread
from twitter.common.log.options import LogOptions
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.quantity import Amount, Time, Data
from twitter.mesos.config.schema import (
  MB,
  MesosJob,
  MesosTaskInstance,
  Task,
  Process,
  Resources)
from twitter.mesos.executor.thermos_executor import ThermosExecutor, ThermosExecutorTimer
from twitter.mesos.executor.task_runner_wrapper import TaskRunnerWrapper
from twitter.mesos.executor.sandbox_manager import DirectorySandbox
from twitter.mesos.executor.status_manager import StatusManager
from twitter.thermos.base.path import TaskPath
from twitter.thermos.monitoring.monitor import TaskMonitor
from twitter.thermos.runner.runner import TaskRunner


if 'THERMOS_DEBUG' in os.environ:
  LogOptions.set_stderr_log_level('google:DEBUG')
  LogOptions.set_simple(True)
  log.init('executor_logger')


class TestStatusManager(StatusManager):
  # this should be greater than TaskRunner.COORDINATOR_INTERVAL_SLEEP (currently 1s) in order to
  # wait until the task cleanly transitions past the CLEANING stage
  WAIT_LIMIT = Amount(1100, Time.MILLISECONDS)
  # time between escalations in the qqq/aaa path
  ESCALATION_WAIT = Amount(1, Time.MILLISECONDS)
  # only necessary with real Mesos driver
  PERSISTENCE_WAIT = Amount(0, Time.SECONDS)


class FastThermosExecutor(ThermosExecutor):
  STOP_WAIT = Amount(0, Time.SECONDS)


class TestThermosExecutorTimer(ThermosExecutorTimer):
  EXECUTOR_TIMEOUT = Amount(100, Time.MILLISECONDS)


class TestTaskRunner(TaskRunnerWrapper):
  def __init__(self, task_id, mesos_task, role, mesos_ports, **kwargs):
    runner_pex = os.path.join('dist', 'thermos_runner.pex')
    sandbox = DirectorySandbox(tempfile.mkdtemp())
    super(TestTaskRunner, self).__init__(
        task_id,
        mesos_task,
        role,
        mesos_ports,
        runner_pex,
        sandbox,
        **kwargs)

  def cleanup(self):
    self._sandbox.destroy()


class FailingStartingTaskRunner(TestTaskRunner):
  def start(self):
    raise self.TaskError('I am an idiot!')


class FailingInitializingTaskRunner(TestTaskRunner):
  def initialize(self):
    raise self.TaskError('I am another idiot!')


class SlowInitializingTaskRunner(TestTaskRunner):
  def __init__(self, *args, **kwargs):
    super(SlowInitializingTaskRunner, self).__init__(*args, **kwargs)
    self.is_initialized = lambda: False
    self._init_start = threading.Event()
    self._init_done = threading.Event()
  def initialize(self):
    self._init_start.wait()
    self.is_initialized = lambda: True
    self._init_done.set()


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


def make_task(thermos_config, assigned_ports={}, **kw):
  at = AssignedTask(task=TwitterTaskInfo(thermosConfig=thermos_config.json_dumps(), **kw),
                    assignedPorts=assigned_ports)
  td = mesos_pb.TaskInfo()
  td.task_id.value = thermos_config.task().name().get() + '-001'
  td.name = thermos_config.task().name().get()
  td.data = serialize(at)
  return td


BASE_MTI = MesosTaskInstance(instance = 0, role = getpass.getuser())
BASE_TASK = Task(resources = Resources(cpu=1.0, ram=16*MB, disk=32*MB))

HELLO_WORLD_TASK_ID = 'hello_world-001'
HELLO_WORLD = BASE_TASK(
    name = 'hello_world',
    processes = [Process(name = 'hello_world_{{thermos.task_id}}', cmdline = 'echo hello world')])
HELLO_WORLD_MTI = BASE_MTI(task=HELLO_WORLD)

SLEEP60 = BASE_TASK(processes = [Process(name = 'sleep60', cmdline = 'sleep 60')])
SLEEP60_MTI = BASE_MTI(task=SLEEP60)

MESOS_JOB = MesosJob(
  name = 'does_not_matter',
  instances = 1,
  role = getpass.getuser(),
)

def test_deserialize_thermos_task():
  assigned_task = AssignedTask(task=TwitterTaskInfo(
      thermosConfig=MESOS_JOB(task=HELLO_WORLD).json_dumps(),
      shardId=0))
  assert ThermosExecutor.deserialize_thermos_task(assigned_task) == BASE_MTI(task=HELLO_WORLD)

  assigned_task = AssignedTask(task=TwitterTaskInfo(
      thermosConfig=HELLO_WORLD_MTI.json_dumps(),
      shardId=0))
  assert ThermosExecutor.deserialize_thermos_task(assigned_task) == BASE_MTI(task=HELLO_WORLD)


def make_runner(proxy_driver, checkpoint_root, task, ports={}, fast_status=False,
                executor_timer_class=TestThermosExecutorTimer):
  runner_class = functools.partial(TestTaskRunner, checkpoint_root=checkpoint_root)
  manager_class = TestStatusManager if fast_status else StatusManager
  te = FastThermosExecutor(runner_class=runner_class, manager_class=manager_class)
  executor_timer_class(te, proxy_driver).start()
  task_description = make_task(task, assigned_ports=ports)
  te.launchTask(proxy_driver, task_description)

  while not te._runner.is_started():
    time.sleep(0.1)

  while len(proxy_driver.method_calls['sendStatusUpdate']) < 2:
    time.sleep(0.1)

  # make sure startup was kosher
  updates = proxy_driver.method_calls['sendStatusUpdate']
  assert len(updates) == 2
  status_updates = [arg_tuple[0][0] for arg_tuple in updates]
  assert status_updates[0].state == mesos_pb.TASK_STARTING
  assert status_updates[1].state == mesos_pb.TASK_RUNNING

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
    if not TestThermosExecutor.PANTS_BUILT:
      assert subprocess.call(["./pants", "src/python/twitter/mesos/executor:thermos_runner"]) == 0
      PANTS_BUILD = True

  @classmethod
  def teardown_class(cls):
    if 'THERMOS_DEBUG' not in os.environ:
      safe_rmtree(cls.LOG_DIR)
    else:
      print('Saving executor logs in %s' % cls.LOG_DIR)

  def test_basic(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as tempdir:
      runner_class = functools.partial(TestTaskRunner, checkpoint_root=tempdir)
      te = ThermosExecutor(runner_class=runner_class)
      te.launchTask(proxy_driver, make_task(HELLO_WORLD_MTI))
      while te._runner.is_alive():
        time.sleep(0.1)
      while te._manager is None:
        time.sleep(0.1)
      te._manager.join()
      tm = TaskMonitor(TaskPath(root=tempdir), task_id=HELLO_WORLD_TASK_ID)
      runner_state = tm.get_state()

    assert 'hello_world_hello_world-001' in runner_state.processes, (
      'Could not find processes, got: %s' % ' '.join(runner_state.processes))

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    status_updates = [arg_tuple[0][0] for arg_tuple in updates]
    assert status_updates[0].state == mesos_pb.TASK_STARTING
    assert status_updates[1].state == mesos_pb.TASK_RUNNING
    assert status_updates[2].state == mesos_pb.TASK_FINISHED

  def test_basic_as_job(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as tempdir:
      runner_class = functools.partial(TestTaskRunner, checkpoint_root=tempdir)
      te = ThermosExecutor(runner_class=runner_class)
      te.launchTask(proxy_driver, make_task(MESOS_JOB(task=HELLO_WORLD), shardId=0))
      while te._runner.is_alive():
        time.sleep(0.1)
      while te._manager is None:
        time.sleep(0.1)
      te._manager.join()
      tm = TaskMonitor(TaskPath(root=tempdir), task_id=HELLO_WORLD_TASK_ID)
      runner_state = tm.get_state()

    assert 'hello_world_hello_world-001' in runner_state.processes, (
      'Could not find processes, got: %s' % ' '.join(runner_state.processes))
    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    status_updates = [arg_tuple[0][0] for arg_tuple in updates]
    assert status_updates[0].state == mesos_pb.TASK_STARTING
    assert status_updates[1].state == mesos_pb.TASK_RUNNING
    assert status_updates[2].state == mesos_pb.TASK_FINISHED

  def test_runner_disappears(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      _, executor = make_runner(proxy_driver, checkpoint_root, SLEEP60_MTI, fast_status=True)
      while executor._runner is None or executor._runner._popen is None or (
          executor._runner._popen.pid is None):
        time.sleep(0.1)
      os.kill(executor._runner._popen.pid, signal.SIGKILL)
      executor._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_LOST

  def test_task_killed(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      runner, executor = make_runner(proxy_driver, checkpoint_root, SLEEP60_MTI)
      runner.kill(force=True, preemption_wait=Amount(1, Time.SECONDS))
      executor._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_KILLED

  def test_killTask(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      _, executor = make_runner(proxy_driver, checkpoint_root, SLEEP60_MTI)
      # send two, expect at most one delivered
      executor.killTask(proxy_driver, mesos_pb.TaskID(value='sleep60-001'))
      executor.killTask(proxy_driver, mesos_pb.TaskID(value='sleep60-001'))
      executor._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_KILLED

  def test_shutdown(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      _, executor = make_runner(proxy_driver, checkpoint_root, SLEEP60_MTI)
      executor.shutdown(proxy_driver)
      executor._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_KILLED

  def test_task_lost(self):
    proxy_driver = ProxyDriver()

    with temporary_dir() as checkpoint_root:
      runner, executor = make_runner(proxy_driver, checkpoint_root, SLEEP60_MTI)
      runner.lose(force=True)
      executor._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_LOST

  def test_task_health_failed(self):
    proxy_driver = ProxyDriver()
    with SignalServer(UnhealthyHandler) as port:
      with temporary_dir() as checkpoint_root:
        _, executor = make_runner(proxy_driver, checkpoint_root,
                                  SLEEP60_MTI(health_check_interval_secs=0.1),
                                  ports={'health': port}, fast_status=True)
        executor._manager.join()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 3
    assert updates[-1][0][0].state == mesos_pb.TASK_FAILED

  def test_failing_runner_start(self):
    proxy_driver = ProxyDriver()

    te = FastThermosExecutor(runner_class=FailingStartingTaskRunner)
    te.launchTask(proxy_driver, make_task(HELLO_WORLD_MTI))

    proxy_driver._stop_event.wait(timeout=1.0)
    assert proxy_driver._stop_event.is_set()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert updates[-1][0][0].state == mesos_pb.TASK_FAILED

  def test_failing_runner_initialize(self):
    proxy_driver = ProxyDriver()

    te = FastThermosExecutor(runner_class=FailingInitializingTaskRunner)
    te.launchTask(proxy_driver, make_task(HELLO_WORLD_MTI))

    proxy_driver._stop_event.wait(timeout=1.0)
    assert proxy_driver._stop_event.is_set()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert updates[-1][0][0].state == mesos_pb.TASK_FAILED

  def test_killTask_during_runner_initialize(self):
    proxy_driver = ProxyDriver()

    task = make_task(HELLO_WORLD_MTI)
    te = FastThermosExecutor(runner_class=SlowInitializingTaskRunner)
    te.launchTask(proxy_driver, task)
    te.killTask(proxy_driver, mesos_pb.TaskID(value=task.task_id.value))
    assert te._abort_runner.is_set()
    assert not te._runner.is_initialized()
    # we've simulated a "slow" initialization by blocking it until the killTask was sent - so now,
    # trigger the initialization to complete
    te._runner._init_start.set()
    # however, wait on the runner to definitely finish its initialization before continuing
    # (otherwise, this function races ahead too fast)
    te._runner._init_done.wait()
    assert te._runner.is_initialized()

    proxy_driver._stop_event.wait(timeout=1.0)
    assert proxy_driver._stop_event.is_set()

    updates = proxy_driver.method_calls['sendStatusUpdate']
    assert len(updates) == 2
    assert updates[-1][0][0].state == mesos_pb.TASK_KILLED

def test_waiting_executor():
  proxy_driver = ProxyDriver()
  with temporary_dir() as checkpoint_root:
    runner_class = functools.partial(TestTaskRunner, checkpoint_root=checkpoint_root)
    te = ThermosExecutor(runner_class=runner_class)
    TestThermosExecutorTimer(te, proxy_driver).start()
    proxy_driver._stop_event.wait(timeout=1.0)
    assert proxy_driver._stop_event.is_set()
