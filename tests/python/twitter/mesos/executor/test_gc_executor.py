from collections import defaultdict
import os
import threading
import time

from thrift.TSerialization import serialize as thrift_serialize
from thrift.TSerialization import deserialize as thrift_deserialize
import mesos_pb2 as mesos

from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_rmtree
from twitter.common.quantity import Amount, Time, Data
from twitter.mesos.executor.gc_executor import ThermosGCExecutor

from gen.twitter.mesos.comm.ttypes import AdjustRetainedTasks, SchedulerMessage
from gen.twitter.mesos.constants import (
    ACTIVE_STATES,
    LIVE_STATES,
    TERMINAL_STATES)
from gen.twitter.mesos.ttypes import ScheduleStatus
from gen.twitter.thermos.ttypes import ProcessState, TaskState


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


if 'THERMOS_DEBUG' in os.environ:
  from twitter.common import log
  from twitter.common.log.options import LogOptions
  LogOptions.set_disk_log_level('NONE')
  LogOptions.set_stderr_log_level('DEBUG')
  log.init('test_gc_executor')


def setup_tree(td, lose=False):
  import shutil
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


class ProxyDriver(object):
  def __init__(self):
    self.stopped = threading.Event()
    self.updates = []
    self.messages = []

  def stop(self):
    self.stopped.set()

  def sendStatusUpdate(self, update):
    self.updates.append((update.state, update.task_id.value, update.message))

  def sendFrameworkMessage(self, message):
    self.messages.append(thrift_deserialize(SchedulerMessage(), message))


def serialize_art(art, task_id='default_task_id'):
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

  def terminate_task(self, task_id, kill=True):
    if kill:
      self._kills.add(task_id)
    else:
      self._losses.add(task_id)
    return True

  @property
  def linked_executors(self):
    return self._active_executors


class ThickTestThermosGCExecutor(ThinTestThermosGCExecutor):
  def __init__(self, active_tasks, finished_tasks, active_executors=[]):
    self._active_tasks = active_tasks
    self._finished_tasks = finished_tasks
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

  def get_state(self, task_id):
    if task_id in self._active_tasks:
      return [(self._clock.time(), self._active_tasks[task_id])]
    elif task_id in self._finished_tasks:
      return [(self._clock.time(), self._finished_tasks[task_id])]
    return []


def make_pair(*args, **kw):
  return ThickTestThermosGCExecutor(*args, **kw), ProxyDriver()


def mix(iter1, iter2):
  for st0 in iter1:
    for st1 in iter2:
      yield (st0, st1)


def lmix(iter1, iter2):
  return list(mix(iter1, iter2))


def llen(*iterables):
  return tuple(len(iterable) for iterable in iterables)


def test_state_reconciliation_no_ops():
  for st0, st1 in mix(THERMOS_LIVES, LIVE_STATES):
    tgc, driver = make_pair({'foo': st0}, {})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)

  for st0, st1 in mix(THERMOS_TERMINALS, TERMINAL_STATES):
    tgc, driver = make_pair({}, {'foo': st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 0)


def test_state_reconciliation_active_terminal():
  for st0, st1 in mix(THERMOS_LIVES, TERMINAL_STATES):
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
  for st0, st1 in mix(THERMOS_TERMINALS, LIVE_STATES):
    tgc, driver = make_pair({}, {'foo': st0})
    lgc, rgc, updates = tgc.reconcile_states(driver, {'foo': st1})
    assert tgc.len_results == (0, 0, 0, 0)
    assert llen(lgc, rgc, updates) == (0, 0, 1)


def test_state_reconciliation_terminal_nexist():
  for st0, st1 in mix(THERMOS_TERMINALS, LIVE_STATES):
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


def test_real_get_state():
  # TODO(wickman) Test actual state reconstruction code.
  pass


def run_gc_with(active_executors, retained_tasks, lose=False):
  proxy_driver = ProxyDriver()
  with temporary_dir() as td:
    setup_tree(td, lose=lose)
    tgce = ThinTestThermosGCExecutor(td, active_executors=active_executors)
    art = AdjustRetainedTasks(retainedTasks=retained_tasks)
    tgce.launchTask(proxy_driver, serialize_art(art, 'gc_executor_task_id'))
    proxy_driver.stopped.wait(timeout=1.0)
    assert proxy_driver.stopped.is_set()
  assert len(proxy_driver.updates) >= 1
  assert proxy_driver.updates[-1][0] == mesos.TASK_FINISHED
  assert proxy_driver.updates[-1][1] == 'gc_executor_task_id'
  return tgce, proxy_driver


def test_gc_with_loss():
  tgce, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS), retained_tasks={}, lose=True)
  assert len(tgce._kills) == len(ACTIVE_TASKS)
  assert len(tgce.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0
  assert len(proxy_driver.updates) == 2
  assert proxy_driver.updates[0][0] == mesos.TASK_LOST
  assert proxy_driver.updates[0][1] == ACTIVE_TASKS[0]


def test_gc_without_task_missing():
  tgce, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS), retained_tasks={}, lose=False)
  assert len(tgce._kills) == len(ACTIVE_TASKS)
  assert len(tgce.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0


def test_gc_without_loss():
  tgce, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS),
      retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.RUNNING})
  assert len(tgce._kills) == 0
  assert len(tgce.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 0


def test_gc_withheld():
  tgce, proxy_driver = run_gc_with(active_executors=set([ACTIVE_TASKS[0], 'failure']),
      retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.RUNNING,
                      'failure': ScheduleStatus.FAILED})
  assert len(tgce._kills) == 0
  assert len(tgce.gcs) == len(FINISHED_TASKS) - 1
  assert len(proxy_driver.messages) == 0

"""
TODO(wickman)  Uncomment when external MESOS-317 is fixed.

def test_gc_withheld_and_executor_missing():
  tgce, proxy_driver = run_gc_with(active_executors=set(ACTIVE_TASKS),
      retained_tasks={ACTIVE_TASKS[0]: ScheduleStatus.RUNNING,
                      'failure': ScheduleStatus.FAILED})
  assert len(tgce._kills) == 0
  assert len(tgce.gcs) == len(FINISHED_TASKS)
  assert len(proxy_driver.messages) == 1
  assert proxy_driver.messages[0].deletedTasks.taskIds == set(['failure'])
"""