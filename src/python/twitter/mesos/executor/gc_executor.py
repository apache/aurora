"""Thermos garbage-collection (GC) executor

This module containts the Thermos GC executor, responsible for garbage collecting old tasks and
reconciling task states with the Mesos scheduler. It is intended to be run periodically on Mesos
slaves utilising the Thermos executor.

"""

import os
import pwd
import threading
import time

from twitter.common import log
from twitter.common.concurrent import defer
from twitter.common.quantity import Amount, Time, Data
from twitter.mesos.common_internal.clusters import TwitterCluster
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

from .sandbox_manager import AppAppSandbox, DirectorySandbox, SandboxBase
from .executor_base import ThermosExecutorBase
from .executor_detector import ExecutorDetector

from thrift.TSerialization import deserialize as thrift_deserialize
from thrift.TSerialization import serialize as thrift_serialize

import psutil


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

  def __init__(self,
               checkpoint_root,
               mesos_root=None,
               verbose=True,
               task_killer=TaskKiller,
               executor_detector=ExecutorDetector,
               task_garbage_collector=TaskGarbageCollector,
               clock=time):
    ThermosExecutorBase.__init__(self)
    self._slave_id = None
    self._mesos_root = mesos_root or TwitterCluster.DEFAULT_MESOS_ROOT
    self._detector = executor_detector()
    self._collector = task_garbage_collector(root=checkpoint_root)
    self._clock = clock
    self._start_time = clock.time()
    self._task_killer = task_killer
    self._checkpoint_root = checkpoint_root
    if 'ANGRYBIRD_THERMOS' in os.environ:
      self._checkpoint_root = os.path.join(os.environ['ANGRYBIRD_THERMOS'], 'thermos', 'run')

  def _runner_ckpt(self, task_id):
    """Return the runner checkpoint file for a given task_id"""
    return TaskPath(root=self._checkpoint_root, task_id=task_id).getpath('runner_checkpoint')

  def terminate_task(self, task_id, kill=True):
    """Terminate a task using the associated task killer. Returns a boolean indicating success."""
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
    statuses = CheckpointDispatcher.iter_statuses(self._runner_ckpt(task_id))
    try:
      return [(state.timestamp_ms / 1000.0, state.state) for state in statuses]
    except CheckpointDispatcher.ErrorRecoveringState:
      return []

  def get_sandbox(self, task_id):
    """Returns the sandbox of the task, or None if it has not yet been initialized."""
    try:
      for update in CheckpointDispatcher.iter_updates(self._runner_ckpt(task_id)):
        if update.runner_header and update.runner_header.sandbox:
          return update.runner_header.sandbox
    except CheckpointDispatcher.ErrorRecoveringState:
      return None

  def maybe_terminate_unknown_task(self, task_id):
    """Terminate a task if we believe the scheduler doesn't know about it.

       It's possible for the scheduler to queue a GC and launch a task afterwards, in which
       case we may see actively running tasks that the scheduler did not report in the
       AdjustRetainedTasks message.

       Returns:
         boolean indicating whether the task was terminated
    """
    states = self.get_states(task_id)
    if states:
      task_start_time, _ = states[0]
      if self._start_time - task_start_time > self.MINIMUM_KILL_AGE.as_(Time.SECONDS):
        return self.terminate_task(task_id)
    return False

  def should_gc_task(self, task_id):
    """Check if a possibly-corrupt task should be locally GCed

      A task should be GCed if its checkpoint stream appears to be corrupted and the kill age
      threshold is exceeded.

       Returns:
         set, containing the task_id if it should be marked for local GC, or empty otherwise
    """
    runner_ckpt = self._runner_ckpt(task_id)
    if not os.path.exists(runner_ckpt):
      return set()
    latest_update = os.path.getmtime(runner_ckpt)
    if self._start_time - latest_update > self.MINIMUM_KILL_AGE.as_(Time.SECONDS):
      self.log('Got corrupt checkpoint file for %s - marking for local GC' % task_id)
      return set([task_id])
    else:
      self.log('Checkpoint file unreadable, but not yet beyond MINIMUM_KILL_AGE threshold')
      return set()

  def reconcile_states(self, driver, retained_tasks):
    """Reconcile states that the scheduler thinks tasks are in vs what they really are in.

        Local    vs   Scheduler  => Action
       ===================================
        ACTIVE         ACTIVE    => no-op
        ACTIVE        STARTING   => no-op
        ACTIVE        TERMINAL   => maybe kill task*
        ACTIVE        !EXISTS    => maybe kill task*
       TERMINAL        ACTIVE    => send actual status**
       TERMINAL       STARTING   => send actual status**
       TERMINAL       TERMINAL   => no-op
       TERMINAL       !EXISTS    => gc locally
       !EXISTS         ACTIVE    => send LOST**
       !EXISTS        STARTING   => no-op
       !EXISTS        TERMINAL   => gc remotely

       * - Only kill if this does not appear to be a race condition.
       ** - These appear to have no effect

       Side effecting operations:
         ACTIVE   | (TERMINAL / !EXISTS) => maybe kill
         TERMINAL | !EXISTS              => delete
         !EXISTS  | TERMINAL             => delete

      Returns tuple of (local_gc, remote_gc, updates), where:
        local_gc - set of task_ids to be GCed locally
        remote_gc - set of task_ids to be deleted on the scheduler
        updates - dictionary of updates sent to the scheduler (task_id: ScheduleStatus)
    """
    def partition(rt):
      active, starting, finished = set(), set(), set()
      for task_id, schedule_status in rt.items():
        if self.twitter_status_is_terminal(schedule_status):
          finished.add(task_id)
        elif (schedule_status == ScheduleStatus.STARTING or
              schedule_status == ScheduleStatus.ASSIGNED):
          starting.add(task_id)
        else:
          active.add(task_id)
      return active, starting, finished

    local_active, local_finished = self.partition_tasks()
    sched_active, sched_starting, sched_finished = partition(retained_tasks)
    local_task_ids = local_active | local_finished
    sched_task_ids = sched_active | sched_starting | sched_finished
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
      if task_id in local_active and task_id not in (sched_active | sched_starting):
        self.log('Inspecting task %s for termination.' % task_id)
        if not self.maybe_terminate_unknown_task(task_id):
          local_gc.update(self.should_gc_task(task_id))
      if task_id in local_finished and task_id not in sched_task_ids:
        self.log('Queueing task %s for local deletion.' % task_id)
        local_gc.add(task_id)
      if task_id in local_finished and task_id in (sched_active | sched_starting):
        self.log('Task %s finished but scheduler thinks active/starting.' % task_id)
        states = self.get_states(task_id)
        if states:
          _, last_state = states[-1]
          updates[task_id] = self.THERMOS_TO_TWITTER_STATES.get(last_state, ScheduleStatus.UNKNOWN)
          self.send_update(driver, task_id, last_state, 'Task finish detected by GC executor.')
        else:
          local_gc.update(self.should_gc_task(task_id))
      if task_id in sched_finished and task_id not in local_task_ids:
        self.log('Queueing task %s for remote deletion.' % task_id)
        remote_gc.add(task_id)
      if task_id not in local_task_ids and task_id in sched_active:
        self.log('Know nothing about task %s, telling scheduler of LOSS.' % task_id)
        updates[task_id] = ScheduleStatus.LOST
        self.send_update(driver, task_id, 'LOST', 'GC executor found no trace of task.')
      if task_id not in local_task_ids and task_id in sched_starting:
        self.log('Know nothing about task %s, but scheduler says STARTING - passing' % task_id)

    return local_gc, remote_gc, updates

  def clean_orphans(self, driver):
    """Inspect checkpoints for trees that have been kill -9'ed but not properly cleaned up."""
    active_tasks, _ = self.partition_tasks()
    updates = {}

    inspector = CheckpointInspector(self._checkpoint_root)

    def is_our_process(process, uid, timestamp):
      if process.uids.real != uid:
        return False
      estimated_start_time = self._clock.time() - process.create_time
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
      try:
        runner_process = psutil.Process(runner_pid)
        if is_our_process(runner_process, runner_uid, timestamp_ms / 1000.0):
          log.info('  - Runner appears healthy.')
          continue
      except psutil.NoSuchProcess:
        # Runner is dead
        pass
      except psutil.Error as err:
        log.error("  - Error sampling runner process: %s" % (runner_pid, err))
        continue
      latest_update = os.path.getmtime(self._runner_ckpt(task_id))
      if self._clock.time() - latest_update < self.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS):
        log.info('  - Runner is dead but under LOST threshold.')
        continue
      log.info('  - Runner is dead but beyond LOST threshold: %.1fs' % (
          self._clock.time() - latest_update))
      if self.terminate_task(task_id, kill=False):
        updates[task_id] = ScheduleStatus.LOST
        self.send_update(driver, task_id, 'LOST', 'GC executor detected failed task runner.')

    return updates

  def _erase_sandbox(self, task_id):
    appapp_sandbox = AppAppSandbox(task_id)
    if appapp_sandbox.exists():
      self.log('Destroying AppAppSandbox for %s' % task_id)
      try:
        appapp_sandbox.destroy()
      except SandboxBase.DeletionError as err:
        self.log('Error destroying AppAppSandbox: %s!' % err)
    else:
      header_sandbox = self.get_sandbox(task_id)
      directory_sandbox = DirectorySandbox(header_sandbox) if header_sandbox else None
      if directory_sandbox and directory_sandbox.exists():
        self.log('Destroying DirectorySandbox for %s' % task_id)
        directory_sandbox.destroy()
      else:
        self.log('Found no sandboxes for %s' % task_id)

  def _gc(self, task_id):
    self.log('Erasing sandbox for %s' % task_id)
    self._erase_sandbox(task_id)
    self.log('Erasing logs for %s' % task_id)
    self._collector.erase_logs(task_id)
    self.log('Erasing metadata for %s' % task_id)
    self._collector.erase_metadata(task_id)

  def garbage_collect(self, force_delete=frozenset()):
    active_tasks, finished_tasks = self.partition_tasks()
    retained_executors = set(iter(self.linked_executors))
    self.log('Executor sandboxes retained by Mesos:')
    for r_e in sorted(retained_executors):
      self.log('  %s' % r_e)
    else:
      self.log('  None')
    for task_id in active_tasks - retained_executors:
      self.log('ERROR: Active task %s had its executor sandbox pulled.' % task_id)
    gc_tasks = finished_tasks - retained_executors
    for gc_task in (gc_tasks | force_delete):
      self._gc(gc_task)
    return gc_tasks

  @property
  def linked_executors(self):
    thermos_executor_prefix = 'thermos-'
    for executor in self._detector.find(root=self._mesos_root):
      # It's possible for just the 'latest' symlink to be present but no run directories.
      # This indicates that the task has been fully garbage collected.
      if executor.executor_id.startswith(thermos_executor_prefix) and executor.run != 'latest':
        yield executor.executor_id[len(thermos_executor_prefix):]

  def run(self, driver, task, retain_tasks):
    """
      retain_tasks: mapping of task_id => ScheduleStatus
    """
    local_gc, remote_gc, _ = self.reconcile_states(driver, retain_tasks)
    self.clean_orphans(driver)
    delete_tasks = set(retain_tasks).intersection(self.garbage_collect(local_gc)) | remote_gc
    if delete_tasks:
      driver.sendFrameworkMessage(thrift_serialize(
          SchedulerMessage(deletedTasks=DeletedTasks(taskIds=delete_tasks))))
    self.send_update(driver, task.task_id.value, 'FINISHED', 'Garbage collection finished.')
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
