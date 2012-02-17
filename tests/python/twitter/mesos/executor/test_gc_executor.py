from collections import defaultdict

from thrift.TSerialization import serialize
import mesos_pb2 as mesos

from twitter.common.quantity import Amount, Time, Data
from gen.twitter.mesos.ttypes import ScheduleStatus
from gen.twitter.mesos.comm.ttypes import AdjustRetainedTasks
from gen.twitter.thermos.ttypes import ProcessState
from twitter.mesos.executor.gc_executor import ThermosGCExecutor


class ProxyDriver(object):
  def __init__(self):
    self.method_calls = defaultdict(list)

  def __getattr__(self, attr):
    def enqueue_arguments(*args, **kw):
      self.method_calls[attr].append((args, kw))
    return enqueue_arguments


class ProxyRunner(object):
  def __init__(self):
    self._kills = set()

  def __call__(self, task_id, checkpoint_root):
    class AnonymousKiller(object):
      def kill(*args, **kw):
        self._kills.add(task_id)
    return AnonymousKiller()


FINISHED_TASKS = {
  'failure': ProcessState.SUCCESS,
  'failure_limit': ProcessState.FAILED,
  'hello_world': ProcessState.SUCCESS,
  'ordering': ProcessState.SUCCESS,
  'ports': ProcessState.SUCCESS,
  'sleep60': ProcessState.KILLED
}

def serialize_art(art):
  td = mesos.TaskDescription()
  td.data = serialize(art)
  return td


class TestThermosGCExecutor(ThermosGCExecutor):
  def __init__(self, *args, **kw):
    ThermosGCExecutor.__init__(self, *args, **kw)
    self._task_garbage_collections = set()

  def garbage_collect_task(self, task_id, task_garbage_collector):
    self._task_garbage_collections.add(task_id)


def test_state_reconciliation():
  proxy_runner = ProxyRunner()
  proxy_driver = ProxyDriver()

  tgce = TestThermosGCExecutor(
    max_age=Amount(10**10, Time.DAYS),
    max_space=Amount(10**10, Data.GB),
    max_tasks=10**10,
    task_runner_factory=proxy_runner,
    checkpoint_root='tests/resources/com/twitter/thermos/root')

  art = AdjustRetainedTasks(retainedTasks = {
    'does_not_exist': ScheduleStatus.RUNNING,
    'failure': ScheduleStatus.FAILED,
    'ordering': ScheduleStatus.FINISHED
  })

  tgce.launchTask(proxy_driver, serialize_art(art))

  assert len(proxy_driver.method_calls['sendStatusUpdate']) == 1
  assert len(proxy_driver.method_calls['sendStatusUpdate'][0]) == 2 # args, kw
  assert len(proxy_driver.method_calls['sendStatusUpdate'][0][0]) == 1 # args
  update = proxy_driver.method_calls['sendStatusUpdate'][0][0][0]
  assert update.task_id.value == 'does_not_exist'
  assert update.state == mesos.TASK_LOST
  assert len(tgce._task_garbage_collections) == 0


def test_gc():
  proxy_runner = ProxyRunner()
  proxy_driver = ProxyDriver()

  tgce = TestThermosGCExecutor(max_tasks=0,
    task_runner_factory=proxy_runner,
    checkpoint_root='tests/resources/com/twitter/thermos/root')

  art = AdjustRetainedTasks(retainedTasks={})
  tgce.launchTask(proxy_driver, serialize_art(art))

  assert len(proxy_driver.method_calls['sendStatusUpdate']) == 0
  assert len(tgce._task_garbage_collections) == len(FINISHED_TASKS)
