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

from gen.twitter.mesos.ttypes import ScheduleStatus
from gen.twitter.mesos.comm.ttypes import AdjustRetainedTasks, SchedulerMessage
from gen.twitter.thermos.ttypes import ProcessState


class ProxyDriver(object):
  def __init__(self):
    self.method_calls = defaultdict(list)
    self.stopped = threading.Event()

  def stop(self):
    self.stopped.set()

  def __getattr__(self, attr):
    def enqueue_arguments(*args, **kw):
      self.method_calls[attr].append((args, kw))
    return enqueue_arguments


class ProxyKiller(object):
  def __init__(self):
    self._kills = set()
    self._loses = set()

  def __call__(self, task_id, checkpoint_root):
    class AnonymousKiller(object):
      def kill(*args, **kw):
        self._kills.add(task_id)
      def lose(*args, **kw):
        self._loses.add(task_id)
    return AnonymousKiller()


ACTIVE_TASKS = ('sleep60-lost',)

FINISHED_TASKS = {
  'failure': ProcessState.SUCCESS,
  'failure_limit': ProcessState.FAILED,
  'hello_world': ProcessState.SUCCESS,
  'ordering': ProcessState.SUCCESS,
  'ports': ProcessState.SUCCESS,
  'sleep60': ProcessState.KILLED
}


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


class TestThermosGCExecutor(ThermosGCExecutor):
  PERSISTENCE_WAIT = Amount(100, Time.MILLISECONDS)

  def __init__(self, *args, **kw):
    ThermosGCExecutor.__init__(self, *args, clock=FakeClock(), **kw)
    self._task_garbage_collections = set()

  def garbage_collect_task(self, task_id, task_garbage_collector):
    self._task_garbage_collections.add(task_id)


def setup_tree(td, lose=False):
  import shutil
  safe_rmtree(td)
  shutil.copytree('tests/resources/com/twitter/thermos/root', td)

  if lose:
    lost_age = time.time() - (
      2 * TestThermosGCExecutor.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS))
    utime = (lost_age, lost_age)
  else:
    utime = None

  # touch everything
  for root, dirs, files in os.walk(td):
    for fn in files:
      os.utime(os.path.join(root, fn), utime)


def test_state_reconciliation():
  proxy_killer = ProxyKiller()
  proxy_driver = ProxyDriver()

  with temporary_dir() as td:
    setup_tree(td, lose=False)
    tgce = TestThermosGCExecutor(
      max_age=Amount(10**10, Time.DAYS),
      max_space=Amount(10**10, Data.GB),
      max_tasks=10**10,
      task_killer=proxy_killer,
      checkpoint_root=td)

    art = AdjustRetainedTasks(retainedTasks = {
      'does_not_exist': ScheduleStatus.RUNNING,
      'failure': ScheduleStatus.FAILED,
      'ordering': ScheduleStatus.FINISHED
    })

    tgce.launchTask(proxy_driver, serialize_art(art, 'gc_executor_task_id'))
    proxy_driver.stopped.wait(timeout=1.0)
    assert proxy_driver.stopped.is_set()

  assert len(proxy_driver.method_calls['sendStatusUpdate']) == 2
  assert len(proxy_driver.method_calls['sendStatusUpdate'][0]) == 2 # args, kw
  assert len(proxy_driver.method_calls['sendStatusUpdate'][0][0]) == 1 # args
  update = proxy_driver.method_calls['sendStatusUpdate'][0][0][0]
  assert update.task_id.value == 'does_not_exist'
  assert update.state == mesos.TASK_LOST
  assert len(tgce._task_garbage_collections) == 0

  update = proxy_driver.method_calls['sendStatusUpdate'][1][0][0]
  assert update.task_id.value == 'gc_executor_task_id'
  assert update.state == mesos.TASK_FINISHED
  assert 'sendFrameworkMessage' not in proxy_driver.method_calls


def test_gc_with_loss():
  proxy_killer = ProxyKiller()
  proxy_driver = ProxyDriver()

  with temporary_dir() as td:
    setup_tree(td, lose=True)
    tgce = TestThermosGCExecutor(max_tasks=0,
      task_killer=proxy_killer,
      checkpoint_root=td)

    art = AdjustRetainedTasks(retainedTasks={})
    tgce.launchTask(proxy_driver, serialize_art(art, 'gc_executor_task_id'))
    proxy_driver.stopped.wait(timeout=1.0)
    assert proxy_driver.stopped.is_set()

  assert len(proxy_driver.method_calls['sendStatusUpdate']) == 2
  assert len(tgce._task_garbage_collections) == len(FINISHED_TASKS)
  update = proxy_driver.method_calls['sendStatusUpdate'][0][0][0]
  assert update.task_id.value == 'sleep60-lost'
  assert update.state == mesos.TASK_LOST

  assert len(proxy_driver.method_calls['sendFrameworkMessage']) == 1
  scheduler_message = SchedulerMessage()
  thrift_deserialize(scheduler_message, proxy_driver.method_calls['sendFrameworkMessage'][0][0][0])
  assert set(tgce._task_garbage_collections) == scheduler_message.deletedTasks.taskIds

  update = proxy_driver.method_calls['sendStatusUpdate'][1][0][0]
  assert update.task_id.value == 'gc_executor_task_id'
  assert update.state == mesos.TASK_FINISHED


def test_gc_without_loss():
  proxy_killer = ProxyKiller()
  proxy_driver = ProxyDriver()

  with temporary_dir() as td:
    setup_tree(td)
    tgce = TestThermosGCExecutor(max_tasks=0,
      task_killer=proxy_killer,
      checkpoint_root=td)

    art = AdjustRetainedTasks(retainedTasks={})
    tgce.launchTask(proxy_driver, serialize_art(art, 'gc_executor_task_id'))
    proxy_driver.stopped.wait(timeout=1.0)
    assert proxy_driver.stopped.is_set()

  assert len(proxy_driver.method_calls['sendStatusUpdate']) == 1
  assert len(tgce._task_garbage_collections) == len(FINISHED_TASKS)

  assert len(proxy_driver.method_calls['sendFrameworkMessage']) == 1
  scheduler_message = SchedulerMessage()
  thrift_deserialize(scheduler_message, proxy_driver.method_calls['sendFrameworkMessage'][0][0][0])
  assert set(tgce._task_garbage_collections) == scheduler_message.deletedTasks.taskIds

  update = proxy_driver.method_calls['sendStatusUpdate'][0][0][0]
  assert update.task_id.value == 'gc_executor_task_id'
  assert update.state == mesos.TASK_FINISHED
