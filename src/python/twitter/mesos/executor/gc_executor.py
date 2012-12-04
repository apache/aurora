import os
import pwd
import threading
import time

from twitter.common import log
from twitter.common.concurrent import defer
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time, Data
from twitter.thermos.base.ckpt import CheckpointDispatcher
from twitter.thermos.base.path import TaskPath
from twitter.thermos.runner.inspector import CheckpointInspector
from twitter.thermos.runner.helper import TaskKiller
from twitter.thermos.monitoring.detector import TaskDetector
from twitter.thermos.monitoring.garbage import TaskGarbageCollector

from gen.twitter.mesos.comm.ttypes import (
    AdjustRetainedTasks,
    DeletedTasks,
    SchedulerMessage)
from gen.twitter.mesos.ttypes import ScheduleStatus

from .sandbox_manager import (
    DirectorySandbox,
    AppAppSandbox)
from .executor_base import ThermosExecutorBase
from .executor_detector import ExecutorDetector

from thrift.TSerialization import deserialize as thrift_deserialize
from thrift.TSerialization import serialize as thrift_serialize


class ThermosGCExecutor(ThermosExecutorBase):
  """
    Thermos GC Executor, responsible for:
      - garbage collecting old tasks to make sure they don't clutter up the system
      - state reconciliation with the scheduler (in case it thinks we're running
        something we're not or vice versa.)
  """
  MAX_PID_TIME_DRIFT = Amount(10, Time.SECONDS)
  MAX_CHECKPOINT_TIME_DRIFT = Amount(1, Time.HOURS)  # maximum runner disconnection time
  PERSISTENCE_WAIT = Amount(5, Time.SECONDS)

  # how old a task must be before we're willing to kill it, assuming that there could be
  # slight races in the following scenario:
  #    launch gc with retained_tasks={t1, t2, t3}
  #    launch task t4
  MINIMUM_KILL_AGE = Amount(10, Time.MINUTES)

  def __init__(self, checkpoint_root, verbose=True, task_killer=TaskKiller, clock=time):
    ThermosExecutorBase.__init__(self)
    self._slave_id = None
    self._detector = ExecutorDetector()
    self._collector = TaskGarbageCollector(root=checkpoint_root)
    self._clock = clock
    self._start_time = clock.time()
    self._task_killer = task_killer
    self._checkpoint_root = checkpoint_root
    if 'ANGRYBIRD_THERMOS' in os.environ:
      self._checkpoint_root = os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos', 'run')

  def terminate_task(self, task_id, kill=True):
    """Terminate a task given its task_id."""
    killer = self._task_killer(task_id, self._checkpoint_root)
    self.log('Terminating %s...' % task_id)
    runner_terminate = killer.kill if kill else killer.lose
    try:
      runner_terminate(force=True)
      return True
    except Exception as e:
      self.log('Could not terminate: %s' % e)
      return False

  def partition_tasks(self):
    """Return active/finished tasks as discovered from the checkpoint root."""
    detector = TaskDetector(root=self._checkpoint_root)
    active_tasks = set(t_id for _, t_id in detector.get_task_ids(state='active'))
    finished_tasks = set(t_id for _, t_id in detector.get_task_ids(state='finished'))
    return active_tasks, finished_tasks

  def get_states(self, task_id):
    """Returns the (timestamp, status) tuples of the task or [] if could not replay."""
    pathspec = TaskPath(root=self._checkpoint_root, task_id=task_id)
    runner_ckpt = pathspec.getpath('runner_checkpoint')
    statuses = CheckpointDispatcher.iter_statuses(runner_ckpt)
    return [(state.timestamp_ms / 1000.0, state.state) for state in statuses]

  def maybe_terminate_unknown_task(self, task_id):
    """Terminate a task if we believe the scheduler doesn't know about it.

       It's possible for the scheduler to queue a GC and launch a task afterwards, in which
       case we may see actively running tasks that the scheduler did not report in the
       AdjustRetainedTasks message.
    """
    states = self.get_states(task_id)
    if len(states) > 0:
      task_start_time, _ = states[0]
      if self._start_time - task_start_time > self.MINIMUM_KILL_AGE.as_(Time.SECONDS):
        self.log('Terminating task %s' % task_id)
        self.terminate_task(task_id)

  def reconcile_states(self, driver, retained_tasks):
    """Reconcile states that the scheduler thinks tasks are in vs what they really are in.

       Local      vs   Scheduler  => Action

        ACTIVE          ACTIVE   => no-op
        ACTIVE         TERMINAL  => maybe kill task*
        ACTIVE         !EXISTS   => maybe kill task*
       TERMINAL         ACTIVE   => send actual status**
       TERMINAL        TERMINAL  => no-op
       TERMINAL        !EXISTS   => gc locally
       !EXISTS          ACTIVE   => send LOST**
       !EXISTS         TERMINAL  => gc remotely

       * - Only kill if this does not appear to be a race condition.
       ** - These appear to have no effect

       Side effecting operations:
         ACTIVE   | (TERMINAL / !EXISTS) => maybe kill
         TERMINAL | !EXISTS              => delete
         !EXISTS  | TERMINAL             => delete
    """
    def partition(rt):
      active, finished = set(), set()
      for task_id, schedule_status in rt.items():
        if self.twitter_status_is_terminal(schedule_status):
          finished.add(task_id)
        else:
          active.add(task_id)
      return active, finished

    local_active, local_finished = self.partition_tasks()
    sched_active, sched_finished = partition(retained_tasks)
    local_task_ids = local_active | local_finished
    sched_task_ids = sched_active | sched_finished
    all_task_ids = local_task_ids | sched_task_ids

    self.log('Told to retain the following task ids:')
    for task_id, schedule_status in retained_tasks.items():
      self.log('  => %s as %s' % (task_id, ScheduleStatus._VALUES_TO_NAMES[schedule_status]))

    self.log('Local active tasks:')
    for task_id in local_active:
      self.log('  => %s' % task_id)

    self.log('Local finished tasks:')
    for task_id in local_finished:
      self.log('  => %s' % task_id)

    local_gc, remote_gc = set(), set()
    updates = {}

    for task_id in all_task_ids:
      if task_id in local_active and task_id not in sched_active:
        self.log('Inspecting %s for termination.' % task_id)
        self.maybe_terminate_unknown_task(task_id)
      if task_id in local_finished and task_id not in sched_task_ids:
        self.log('Queueing task_id %s for local deletion.' % task_id)
        local_gc.add(task_id)
      if task_id in local_finished and task_id in sched_active:
        self.log('Task %s finished but scheduler thinks active.' % task_id)
        states = self.get_state(task_id)
        if len(states) > 0:
          _, last_state = states[-1]
          updates[task_id] = self.THERMOS_TO_TWITTER_STATES.get(last_state, ScheduleStatus.UNKNOWN)
          self.send_update(driver, task_id, last_state,
              'Telling scheduler of finished task %s' % task_id)
        else:
          self.log('Task state unavailable for finished task!  Possible checkpoint corruption.')
      if task_id in sched_finished and task_id not in local_task_ids:
        self.log('Queueing task_id %s for remote deletion.' % task_id)
        remote_gc.add(task_id)
      if task_id not in local_task_ids and task_id in sched_active:
        self.log('Know nothing about task %s, telling scheduler of LOSS.' % task_id)
        updates[task_id] = ScheduleStatus.LOST
        self.send_update(driver, task_id, 'LOST', 'Executor found no trace of %s' % task_id)

    return local_gc, remote_gc, updates

  def clean_orphans(self, driver):
    """Inspect checkpoints for trees that have been kill -9'ed but not properly cleaned up."""
    active_tasks, _ = self.partition_tasks()
    updates = {}

    inspector = CheckpointInspector(self._checkpoint_root)
    ps = ProcessProviderFactory.get()
    ps.collect_all()

    # TODO(wickman) This is almost a replica of code in runner/runner.py, factor out.
    def is_our_pid(pid, uid, timestamp):
      handle = ps.get_handle(pid)
      if handle.user() != pwd.getpwuid(uid)[0]: return False
      estimated_start_time = self._clock.time() - handle.wall_time()
      return abs(timestamp - estimated_start_time) < self.MAX_PID_TIME_DRIFT.as_(Time.SECONDS)

    for task_id in active_tasks:
      log.info('Inspecting running task: %s' % task_id)
      inspection = inspector.inspect(task_id)
      latest_runner = inspection.runner_processes[-1]
      # Assume that it has not yet started?
      if not latest_runner:
        log.warning('  - Task has no registered runners.')
        continue
      runner_pid, runner_uid, timestamp_ms = latest_runner
      if runner_pid in ps.pids() and is_our_pid(runner_pid, runner_uid, timestamp_ms / 1000.0):
        log.info('  - Runner appears healthy.')
        continue
      # Runner is dead
      runner_ckpt = TaskPath(root=self._checkpoint_root, task_id=task_id).getpath(
          'runner_checkpoint')
      latest_update = os.path.getmtime(runner_ckpt)
      if self._clock.time() - latest_update < self.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS):
        log.info('  - Runner is dead but under LOST threshold.')
        continue
      log.info('  - Runner is dead but beyond LOST threshold: %.1fs' % (
          self._clock.time() - latest_update))
      if self.terminate_task(task_id, kill=False):
        updates[task_id] = ScheduleStatus.LOST
        self.send_update(driver, task_id, 'LOST',
          'Reporting task %s as LOST because its runner has been dead too long.' % task_id)

    return updates

  def _gc(self, task_id):
    directory_sandbox = DirectorySandbox(task_id)
    if directory_sandbox.exists():
      self.log('Destroying DirectorySandbox for %s' % task_id)
      directory_sandbox.destroy()
    else:
      appapp_sandbox = AppAppSandbox(task_id)
      if appapp_sandbox.exists():
        self.log('Destroying AppAppSandbox for %s' % task_id)
        appapp_sandbox.destroy()
      else:
        self.log('ERROR: Could not identify the sandbox manager for %s!' % task_id)
    self.log('Erasing logs for %s' % task_id)
    self._collector.erase_logs(task_id)
    self.log('Erasing metadata for %s' % task_id)
    self._collector.erase_metadata(task_id)

  def garbage_collect(self, force_delete=frozenset()):
    retained_executors = set(iter(self.linked_executors))
    active_tasks, finished_tasks = self.partition_tasks()
    for task_id in active_tasks - retained_executors:
      self.log('ERROR: Active task %s had its executor sandbox pulled.' % task_id)
    gc_tasks = finished_tasks - retained_executors
    for gc_task in gc_tasks | force_delete:
      self._gc(gc_task)
    return gc_tasks

  @property
  def linked_executors(self):
    thermos_executor_prefix = 'thermos-'
    for executor in self._detector:
      if executor.executor_id.startswith(thermos_executor_prefix):
        yield executor.executor_id[len(thermos_executor_prefix):]

  def run(self, driver, task, retain_tasks):
    local_gc, remote_gc, _ = self.reconcile_states(driver, retain_tasks)
    self.clean_orphans(driver)
    delete_tasks = set(retain_tasks).intersection(self.garbage_collect(local_gc)) | remote_gc
    # TODO(wickman) Stop sending deletedTasks messages temporarily until
    # https://issues.apache.org/jira/browse/MESOS-317 is fixed.
    #
    # if delete_tasks:
    #  driver.sendFrameworkMessage(thrift_serialize(
    #      SchedulerMessage(deletedTasks=DeletedTasks(taskIds=delete_tasks))))
    self.send_update(driver, task.task_id.value, 'FINISHED', "Garbage collection finished.")
    defer(driver.stop, clock=self._clock, delay=self.PERSISTENCE_WAIT)

  def launchTask(self, driver, task):
    self._slave_id = task.slave_id.value
    self.log('launchTask called.')
    retain_tasks = AdjustRetainedTasks()
    thrift_deserialize(retain_tasks, task.data)
    defer(lambda: self.run(driver, task, retain_tasks.retainedTasks))

  def killTask(self, driver, task_id):
    self.log('killTask() got task_id: %s, ignoring.' % task_id)

  def shutdown(self, driver):
    self.log('shutdown() called, ignoring.')
