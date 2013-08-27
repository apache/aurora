from collections import namedtuple
from itertools import product
import os
import shutil
import threading
import time
import unittest

from twitter.common.concurrent import deadline, Timeout
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.quantity import Amount, Time, Data

from twitter.aurora.executor.executor_detector import ExecutorDetector
from twitter.aurora.executor.gc_executor import ThermosGCExecutor
from twitter.aurora.executor.sandbox_manager import AppAppSandbox
from twitter.thermos.config.schema import SimpleTask
from twitter.thermos.runner import TaskRunner

from gen.twitter.aurora.comm.ttypes import AdjustRetainedTasks, SchedulerMessage
from gen.twitter.aurora.constants import ACTIVE_STATES, LIVE_STATES, TERMINAL_STATES
from gen.twitter.aurora.ttypes import ScheduleStatus
from gen.twitter.thermos.ttypes import ProcessState, TaskState

import mock
import mox
from thrift.TSerialization import serialize as thrift_serialize
from thrift.TSerialization import deserialize as thrift_deserialize
import mesos_pb2 as mesos



ACTIVE_TASKS = ('sleep60-lost',)

FINISHED_TASKS = {
  'failure': ProcessState.SUCCESS,
  'failure_limit': ProcessState.FAILED,
  'hello_world': ProcessState.SUCCESS,
  'ordering': ProcessState.SUCCESS,
  'ports': ProcessState.SUCCESS,
  'sleep60': ProcessState.KILLED
}

# TODO(wickman) These should be constant sets in the Thermos thrift
THERMOS_LIVES = (TaskState.ACTIVE, TaskState.CLEANING, TaskState.FINALIZING)
THERMOS_TERMINALS = (TaskState.SUCCESS, TaskState.FAILED, TaskState.KILLED, TaskState.LOST)

STARTING_STATES = (ScheduleStatus.STARTING, ScheduleStatus.ASSIGNED)

TASK_ID = 'gc_executor_task_id'


if 'THERMOS_DEBUG' in os.environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_disk_log_level('NONE')
  LogOptions.set_stderr_log_level('DEBUG')
  log.init('test_gc_executor')


def thread_yield():
  time.sleep(0.1)


def setup_tree(td, lose=False):
  safe_rmtree(td)
  shutil.copytree('tests/resources/com/twitter/thermos/root', td)

  if lose:
    lost_age = time.time() - (
      2 * ThinTestThermosGCExecutor.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS))
    utime = (lost_age, lost_age)
  else:
    utime = None

  # touch everything
  for root, dirs, files in os.walk(td):
    for fn in files:
      os.utime(os.path.join(root, fn), utime)


StatusUpdate = namedtuple('StatusUpdate', 'state task_id')


class ProxyDriver(object):
  def __init__(self):
    self.stopped = threading.Event()
    self.updates = []
    self.messages = []

  def stop(self):
    self.stopped.set()

  def sendStatusUpdate(self, update):
    self.updates.append(StatusUpdate(update.state, update.task_id.value))

  def sendFrameworkMessage(self, message):
    self.messages.append(thrift_deserialize(SchedulerMessage(), message))


def serialize_art(art, task_id=TASK_ID):
  td = mesos.TaskInfo()
  td.slave_id.value = 'ignore_me'
  td.task_id.value = task_id
  td.data = thrift_serialize(art)
  return td


class FakeClock(object):
  def __init__(self):
    self.slept = 0

  def time(self):
    return time.time()

  def sleep(self, amount):
    self.slept += amount



class ThinTestThermosGCExecutor(ThermosGCExecutor):
  POLL_WAIT = Amount(1, Time.MICROSECONDS)
  def __init__(self, checkpoint_root, active_executors=[]):
    self._active_executors = active_executors
    self._clock = FakeClock()
    self._kills = set()
    self._losses = set()
    self._gcs = set()
    ThermosGCExecutor.__init__(self, checkpoint_root, clock=self._clock)

  @property
  def gcs(self):
    return self._gcs

  def _gc(self, task_id):
    self._gcs.add(task_id)

  def _terminate_task(self, task_id, kill=True):
    if kill:
      self._kills.add(task_id)
    else:
      self._losses.add(task_id)
    return True

  @property
  def linked_executors(self):
    return self._active_executors


class ThickTestThermosGCExecutor(ThinTestThermosGCExecutor):
  def __init__(self, active_tasks, finished_tasks, active_executors=[], corrupt_tasks=[]):
    self._active_tasks = active_tasks
    self._finished_tasks = finished_tasks
    self._corrupt_tasks = corrupt_tasks
    self._maybe_terminate = set()
    ThinTestThermosGCExecutor.__init__(self, None, active_executors)

  @property
  def results(self):
    return self._kills, self._losses, self._gcs, self._maybe_terminate

  @property
  def len_results(self):
    return len(self._kills), len(self._losses), len(self._gcs), len(self._maybe_terminate)

  def partition_tasks(self):
    return set(self._active_tasks.keys()), set(self._finished_tasks.keys())

  def maybe_terminate_unknown_task(self, task_id):
    self._maybe_terminate.add(task_id)

  def get_states(self, task_id):
    if task_id not in self._corrupt_tasks:
      if task_id in self._active_tasks:
        return [(self._clock.time(), self._active_tasks[task_id])]
      elif task_id in self._finished_tasks:
        return [(self._clock.time(), self._finished_tasks[task_id])]
    return []

  def should_gc_task(self, task_id):
    if task_id in self._corrupt_tasks:
      return set([task_id])
    return set()


def make_pair(*args, **kw):
  return ThickTestThermosGCExecutor(*args, **kw), ProxyDriver()


def llen(*iterables):
  return tuple(len(iterable) for iterable in iterables)


def test_state_reconciliation_no_ops():
  # active vs. active
  for st0, st1 in product(THERMOS_LIVES, LIVE_STATES):
    tgc, driver = make_pair({'foo': st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  # terminal vs. terminal
  for st0, st1 in product(THERMOS_TERMINALS, TERMINAL_STATES):
    tgc, driver = make_pair({}, {'foo': st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  # active vs. starting
  for st0, st1 in product(THERMOS_LIVES, STARTING_STATES):
    tgc, driver = make_pair({'foo': st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  # nexist vs. starting
  for st1 in STARTING_STATES:
    tgc, driver = make_pair({}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_active_terminal():
  for st0, st1 in product(THERMOS_LIVES, TERMINAL_STATES):
    tgc, driver = make_pair({'foo': st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 1)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_active_nexist():
  for st0 in THERMOS_LIVES:
    tgc, driver = make_pair({'foo': st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {})
    assert tgc.len_results == (0, 0, 0, 1)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_terminal_active():
  for st0, st1 in product(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {'foo': st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 1)

def test_state_reconciliation_corrupt_tasks():
  for st0, st1 in product(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {'foo': st0}, corrupt_tasks=['foo'])
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (1, 0, 0)

def test_state_reconciliation_terminal_nexist():
  for st0, st1 in product(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {'foo': st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (1, 0, 0)
    assert lgc == set(['foo'])


def test_state_reconciliation_nexist_active():
  for st1 in LIVE_STATES:
    tgc, driver = make_pair({}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 1)


def test_state_reconciliation_nexist_terminal():
  for st1 in TERMINAL_STATES:
    tgc, driver = make_pair({}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 1, 0)
    assert rgc == set(['foo'])


def test_real_get_states():
  with temporary_dir() as td:
    setup_tree(td)
    executor = ThinTestThermosGCExecutor(td)
    for task in FINISHED_TASKS:
      states = executor.get_states(task)
      assert isinstance(states, list) and len(states) > 0
      assert executor.get_sandbox(task) is not None


def wait_until_not(thing, clock=time, timeout=0.5):
  """wait until something is booleany False"""
  def wait():
    while thing:
      clock.sleep(0.01)
  try:
    deadline(wait, timeout=timeout, daemon=True)
  except Timeout:
    pass


def run_gc_with(active_executors, retained_tasks, lose=False):
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    setup_tree(td, lose=lose)
    executor = ThinTestThermosGCExecutor(td, active_executors=active_executors)
    executor.registered(proxy_driver, None, None, None)
    executor.start()
    art = AdjustRetainedTasks(retainedTasks=retained_tasks)
    executor.launchTask(proxy_driver, serialize_art(art, TASK_ID))
    wait_until_not(executor._gc_task_queue)
    wait_until_not(executor._task_id)
    assert len(executor._gc_task_queue) == 0
    assert not executor._task_id
  assert len(proxy_driver.updates) >= 1
  if not lose: # if the task is lost it will be cleaned out of band (by clean_orphans),
               # so we don't care when the GC task actually finishes
    assert proxy_driver.updates[-1][0] == mesos.TASK_FINISHED
    assert proxy_driver.updates[-1][1] == TASK_ID
  return executor, proxy_driver


def test_gc_with_loss():
  executor, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS), retained_tasks={},
      lose=True)
  assert len(executor._kills) == len(ACTIVE_TASKS)
  assert len(executor.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0
  assert len(proxy_driver.updates) >= 1
  assert StatusUpdate(mesos.TASK_LOST, ACTIVE_TASKS[0]) in proxy_driver.updates


def test_gc_with_starting_task():
  executor, proxy_driver = run_gc_with(
    active_executors=set(ACTIVE_TASKS), retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.STARTING})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0


def test_gc_without_task_missing():
  executor, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS), retained_tasks={},
      lose=False)
  assert len(executor._kills) == len(ACTIVE_TASKS)
  assert len(executor.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0


def test_gc_without_loss():
  executor, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS),
      retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.RUNNING})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0


def test_gc_withheld():
  executor, proxy_driver = run_gc_with(active_executors=set([ACTIVE_TASKS[0], 'failure']),
      retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.RUNNING,
                      'failure': ScheduleStatus.FAILED})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS) - 1
  assert len(proxy_driver.messages) == 0


def test_gc_withheld_and_executor_missing():
  executor, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS),
      retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.RUNNING,
                      'failure': ScheduleStatus.FAILED})
  assert len(executor._kills) == 0
  assert len(executor.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 1
  assert proxy_driver.messages[0].deletedTasks.taskIds == set(['failure'])


def build_blocking_gc_executor(td, proxy_driver):
  class LongGCThinTestThermosGCExecutor(ThinTestThermosGCExecutor):
    def _run_gc(self, task, retain_tasks):
      # just block until we shutdown
      self._task_id = task.task_id.value
      self._stop_event.wait()
      self._task_id = None
  executor = LongGCThinTestThermosGCExecutor(td)
  executor.registered(proxy_driver, None, None, None)
  executor.start()
  return executor


def test_gc_killtask_noop():
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = ThinTestThermosGCExecutor(td)
    executor.registered(proxy_driver, None, None, None)
    executor.start()
    executor.killTask(proxy_driver, TASK_ID)
  assert not proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) == 0


def test_gc_killtask_current():
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks()))
    wait_until_not(executor._gc_task_queue)
    assert len(executor._gc_task_queue) == 0
    assert executor._task_id == TASK_ID
    executor.killTask(proxy_driver, TASK_ID)
    assert executor._task_id == TASK_ID
    assert len(executor._gc_task_queue) == 0
  assert not proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) == 0


def test_gc_killtask_queued():
  TASK2_ID = "task2"
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks()))
    thread_yield()
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK2_ID))
    thread_yield()
    assert len(executor._gc_task_queue) == 1
    executor.killTask(proxy_driver, TASK2_ID)
    thread_yield()
    assert len(executor._gc_task_queue) == 0
  assert not proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) == 0


def test_gc_multiple_launchtasks():
  TASK2, TASK3 = "task2", "task3"
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = build_blocking_gc_executor(td, proxy_driver)
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks()))
    thread_yield()
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK2))
    thread_yield()
    assert len(executor._gc_task_queue) == 1
    executor.launchTask(proxy_driver, serialize_art(AdjustRetainedTasks(), task_id=TASK3))
    thread_yield()
    assert len(executor._gc_task_queue) == 1
  assert not proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) >= 1
  assert StatusUpdate(mesos.TASK_FINISHED, TASK2) in proxy_driver.updates


def test_gc_shutdown():
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    executor = ThinTestThermosGCExecutor(td)
    executor.registered(proxy_driver, None, None, None)
    executor.start()
    executor.shutdown(proxy_driver)
    executor._stop_event.wait(timeout=1.0)
    assert executor._stop_event.is_set()
  proxy_driver.stopped.wait(timeout=1.0)
  assert proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) == 0


APPAPP_SANDBOX = 'twitter.aurora.executor.gc_executor.AppAppSandbox'
DIRECTORY_SANDBOX = 'twitter.aurora.executor.gc_executor.DirectorySandbox'

class TestRealGC(unittest.TestCase):
  """
    Test functions against the actual garbage_collect() functionality of the GC executor
  """
  def setUp(self):
    self.clock = FakeClock()
    self.HELLO_WORLD= SimpleTask(name="foo", command="echo hello world")

  def setup_task(self, task, root, finished=False, corrupt=False):
    """Set up the checkpoint stream for the given task in the given checkpoint root, optionally
    finished and/or with a corrupt stream"""
    class FastTaskRunner(TaskRunner):
      COORDINATOR_INTERVAL_SLEEP = Amount(1, Time.MICROSECONDS)
    tr = FastTaskRunner(
        task=task,
        checkpoint_root=root,
        sandbox=os.path.join(root, 'sandbox', task.name().get()),
        clock=self.clock)
    with tr.control():
      # initialize checkpoint stream
      pass
    if finished:
      tr.kill()
    if corrupt:
      cpkt_file = TaskPath(root=root, tr=tr.task_id).getpath('runner_checkpoint')
      with open(ckpt_file, 'w') as f:
        f.write("definitely not a valid checkpoint stream")
    return tr.task_id

  def run_gc(self, root, task_id, retain=False):
    """Run the garbage collection process against the given task_id in the given checkpoint root"""
    class FakeTaskKiller(object):
      def __init__(self, task_id, checkpoint_root): pass
      def kill(self): pass
      def lose(self): pass
    class FakeTaskGarbageCollector(object):
      def __init__(self, root): pass
      def erase_logs(self, task_id): pass
      def erase_metadata(self, task_id): pass
    class FakeExecutorDetector(object):
      class ExecutorScanf(object):
        executor_id = 'thermos-' + task_id
        run = 'some_run_number'
      def find(self, root):
        return [self.ExecutorScanf()]
    class FastThermosGCExecutor(ThermosGCExecutor):
      POLL_WAIT = Amount(1, Time.MICROSECONDS)
    executor = FastThermosGCExecutor(
        checkpoint_root=root,
        task_killer=FakeTaskKiller,
        executor_detector=FakeExecutorDetector if retain else ExecutorDetector,
        task_garbage_collector=FakeTaskGarbageCollector,
        clock=self.clock)
    return executor.garbage_collect()

  def test_active_task_no_runners(self):
    # TODO(jon): implement
    pass

  def test_active_task_running(self):
    # TODO(jon): implement
    pass

  def test_finished_task_corrupt(self):
    # TODO(jon): implement
    pass

  def test_gc_task_no_sandbox(self):
    with mock.patch(APPAPP_SANDBOX) as appapp_mock:
      with mock.patch(DIRECTORY_SANDBOX) as directory_mock:
        appapp_sandbox = appapp_mock.return_value
        directory_sandbox = directory_mock.return_value
        appapp_sandbox.exists.return_value = directory_sandbox.exists.return_value = False
        with temporary_dir() as root:
          task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
          gcs = self.run_gc(root, task_id)
          appapp_sandbox.exists.assert_called_with()
          directory_sandbox.exists.assert_called_with()
          assert len(gcs) == 1

  def test_gc_task_appapp_sandbox(self):
    with mock.patch(APPAPP_SANDBOX) as appapp_mock:
      appapp_sandbox = appapp_mock.return_value
      appapp_sandbox.exists.return_value = True
      with temporary_dir() as root:
        task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
        gcs = self.run_gc(root, task_id)
        appapp_sandbox.exists.assert_called_with()
        appapp_sandbox.destroy.assert_called_with()
        assert len(gcs) == 1

  def test_gc_task_appapp_sandbox_quiet_failure(self):
    with mock.patch(APPAPP_SANDBOX) as appapp_mock:
      appapp_sandbox = appapp_mock.return_value
      appapp_sandbox.exists.return_value = True
      appapp_sandbox.destroy.side_effect = AppAppSandbox.DeletionError()
      with temporary_dir() as root:
        task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
        gcs = self.run_gc(root, task_id)
        appapp_sandbox.exists.assert_called_with()
        appapp_sandbox.destroy.assert_called_with()
        assert len(gcs) == 1

  def test_gc_task_directory_sandbox(self):
    with mock.patch(APPAPP_SANDBOX) as appapp_mock:
      with mock.patch(DIRECTORY_SANDBOX) as directory_mock:
        appapp_sandbox = appapp_mock.return_value
        appapp_sandbox.exists.return_value = False
        directory_sandbox = directory_mock.return_value
        directory_sandbox.exists.return_value = True
        with temporary_dir() as root:
          task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
          gcs = self.run_gc(root, task_id)
          directory_sandbox.exists.assert_called_with()
          directory_sandbox.destroy.assert_called_with()
          assert len(gcs) == 1

  def test_gc_ignore_retained_task(self):
    with temporary_dir() as root:
      task_id = self.setup_task(self.HELLO_WORLD, root, finished=True)
      gcs = self.run_gc(root, task_id, retain=True)
      assert len(gcs) == 0

