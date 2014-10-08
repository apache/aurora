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

"""Thermos garbage-collection (GC) executor

This module containts the Thermos GC executor, responsible for garbage collecting old tasks and
reconciling task states with the Mesos scheduler. It is intended to be run periodically on Mesos
slaves utilising the Thermos executor.

"""

import os
import threading
import time

import psutil
from mesos.interface import mesos_pb2
from thrift.TSerialization import deserialize as thrift_deserialize
from twitter.common.collections import OrderedDict
from twitter.common.exceptions import ExceptionalThread
from twitter.common.metrics import Observable
from twitter.common.metrics.gauge import AtomicGauge
from twitter.common.quantity import Amount, Time

from apache.thermos.common.ckpt import CheckpointDispatcher
from apache.thermos.common.path import TaskPath
from apache.thermos.core.helper import TaskKiller
from apache.thermos.core.inspector import CheckpointInspector
from apache.thermos.monitoring.detector import TaskDetector
from apache.thermos.monitoring.garbage import TaskGarbageCollector

from .common.sandbox import DirectorySandbox
from .executor_base import ExecutorBase
from .executor_detector import ExecutorDetector

from gen.apache.aurora.api.constants import TERMINAL_STATES
from gen.apache.aurora.api.ttypes import ScheduleStatus
from gen.apache.aurora.comm.ttypes import AdjustRetainedTasks
from gen.apache.thermos.ttypes import TaskState

THERMOS_TO_TWITTER_STATES = {
    TaskState.ACTIVE: ScheduleStatus.RUNNING,
    TaskState.CLEANING: ScheduleStatus.RUNNING,
    TaskState.FINALIZING: ScheduleStatus.RUNNING,
    TaskState.SUCCESS: ScheduleStatus.FINISHED,
    TaskState.FAILED: ScheduleStatus.FAILED,
    TaskState.KILLED: ScheduleStatus.KILLED,
    TaskState.LOST: ScheduleStatus.LOST,
}


THERMOS_TO_MESOS_STATES = {
    TaskState.ACTIVE: mesos_pb2.TASK_RUNNING,
    TaskState.SUCCESS: mesos_pb2.TASK_FINISHED,
    TaskState.FAILED: mesos_pb2.TASK_FAILED,
    TaskState.KILLED: mesos_pb2.TASK_KILLED,
    TaskState.LOST: mesos_pb2.TASK_LOST,
}


class ThermosGCExecutor(ExecutorBase, ExceptionalThread, Observable):
  """
    Thermos GC Executor, responsible for:
      - garbage collecting old tasks to make sure they don't clutter up the system
      - state reconciliation with the scheduler (in case it thinks we're running
        something we're not or vice versa.)
  """
  MAX_PID_TIME_DRIFT = Amount(10, Time.SECONDS)
  MAX_CHECKPOINT_TIME_DRIFT = Amount(1, Time.HOURS)  # maximum runner disconnection time

  # how old a task must be before we're willing to kill it, assuming that there could be
  # slight races in the following scenario:
  #    launch gc with retained_tasks={t1, t2, t3}
  #    launch task t4
  MINIMUM_KILL_AGE = Amount(10, Time.MINUTES)

  # wait time between checking for new GC events from the slave and/or cleaning orphaned tasks
  POLL_WAIT = Amount(5, Time.MINUTES)

  # maximum lifetime of this executor.  this is to prevent older GC executor binaries from
  # running forever
  MAXIMUM_EXECUTOR_LIFETIME = Amount(1, Time.DAYS)

  PERSISTENCE_WAIT = Amount(5, Time.SECONDS)

  def __init__(self,
               checkpoint_root,
               verbose=True,
               task_killer=TaskKiller,
               executor_detector=ExecutorDetector,
               task_garbage_collector=TaskGarbageCollector,
               clock=time):
    ExecutorBase.__init__(self)
    ExceptionalThread.__init__(self)
    self.daemon = True
    self._stop_event = threading.Event()
    # mapping of task_id => (TaskInfo, AdjustRetainedTasks), in the order in
    # which they were received via a launchTask.
    self._gc_task_queue = OrderedDict()
    # cache the ExecutorDriver provided by the slave, so we can use it out
    # of band from slave-initiated callbacks.  This should be supplied by
    # ExecutorBase.registered() when the executor first registers with the
    # slave.
    self._driver = None
    self._slave_id = None  # cache the slave ID provided by the slave
    self._task_id = None  # the task_id currently being executed by the ThermosGCExecutor, if any
    self._start_time = None  # the start time of a task currently being executed, if any
    self._detector = executor_detector()
    self._collector = task_garbage_collector(root=checkpoint_root)
    self._clock = clock
    self._task_killer = task_killer
    self._checkpoint_root = checkpoint_root
    self._dropped_tasks = AtomicGauge('dropped_tasks')
    self.metrics.register(self._dropped_tasks)

  def _runner_ckpt(self, task_id):
    """Return the runner checkpoint file for a given task_id."""
    return TaskPath(root=self._checkpoint_root, task_id=task_id).getpath('runner_checkpoint')

  def _terminate_task(self, task_id, kill=True):
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
        return self._terminate_task(task_id)
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
        if schedule_status in TERMINAL_STATES:
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
      self.log('  => %s as %s' %
          (task_id, ScheduleStatus._VALUES_TO_NAMES.get(schedule_status, 'UNKNOWN')))

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
          updates[task_id] = THERMOS_TO_TWITTER_STATES.get(
              last_state,
              ScheduleStatus.SANDBOX_DELETED)
          self.send_update(
              driver,
              task_id,
              THERMOS_TO_MESOS_STATES.get(last_state, mesos_pb2.TASK_LOST),
              'Task finish detected by GC executor.')
        else:
          local_gc.update(self.should_gc_task(task_id))
      if task_id in sched_finished and task_id not in local_task_ids:
        self.log('Queueing task %s for remote deletion.' % task_id)
        remote_gc.add(task_id)
      if task_id not in local_task_ids and task_id in sched_active:
        self.log('Know nothing about task %s, telling scheduler of LOSS.' % task_id)
        updates[task_id] = ScheduleStatus.LOST
        self.send_update(
            driver, task_id, mesos_pb2.TASK_LOST, 'GC executor found no trace of task.')
      if task_id not in local_task_ids and task_id in sched_starting:
        self.log('Know nothing about task %s, but scheduler says STARTING - passing' % task_id)

    return local_gc, remote_gc, updates

  def clean_orphans(self, driver):
    """Inspect checkpoints for trees that have been kill -9'ed but not properly cleaned up."""
    self.log('Checking for orphaned tasks')
    active_tasks, _ = self.partition_tasks()
    updates = {}

    inspector = CheckpointInspector(self._checkpoint_root)

    def is_our_process(process, uid, timestamp):
      if process.uids().real != uid:
        return False
      estimated_start_time = self._clock.time() - process.create_time()
      return abs(timestamp - estimated_start_time) < self.MAX_PID_TIME_DRIFT.as_(Time.SECONDS)

    for task_id in active_tasks:
      self.log('Inspecting running task: %s' % task_id)
      inspection = inspector.inspect(task_id)
      if not inspection:
        self.log('  - Error inspecting task runner')
        continue
      latest_runner = inspection.runner_processes[-1]
      # Assume that it has not yet started?
      if not latest_runner:
        self.log('  - Task has no registered runners.')
        continue
      runner_pid, runner_uid, timestamp_ms = latest_runner
      try:
        runner_process = psutil.Process(runner_pid)
        if is_our_process(runner_process, runner_uid, timestamp_ms / 1000.0):
          self.log('  - Runner appears healthy.')
          continue
      except psutil.NoSuchProcess:
        # Runner is dead
        pass
      except psutil.Error as err:
        self.log('  - Error sampling runner process [pid=%s]: %s' % (runner_pid, err))
        continue
      try:
        latest_update = os.path.getmtime(self._runner_ckpt(task_id))
      except (IOError, OSError) as err:
        self.log('  - Error accessing runner ckpt: %s' % err)
        continue
      if self._clock.time() - latest_update < self.MAX_CHECKPOINT_TIME_DRIFT.as_(Time.SECONDS):
        self.log('  - Runner is dead but under LOST threshold.')
        continue
      self.log('  - Runner is dead but beyond LOST threshold: %.1fs' % (
          self._clock.time() - latest_update))
      if self._terminate_task(task_id, kill=False):
        updates[task_id] = ScheduleStatus.LOST
        self.send_update(
            driver, task_id, mesos_pb2.TASK_LOST, 'GC executor detected failed task runner.')

    return updates

  def _erase_sandbox(self, task_id):
    # TODO(wickman) Only mesos should be in the business of garbage collecting sandboxes.
    header_sandbox = self.get_sandbox(task_id)
    directory_sandbox = DirectorySandbox(header_sandbox) if header_sandbox else None
    if directory_sandbox and directory_sandbox.exists():
      self.log('Destroying DirectorySandbox for %s' % task_id)
      try:
        directory_sandbox.destroy()
      except DirectorySandbox.Error as e:
        self.log('Failed to destroy DirectorySandbox: %s' % e)
    else:
      self.log('Found no sandboxes for %s' % task_id)

  def _gc(self, task_id):
    """Erase the sandbox, logs and metadata of the given task."""
    self.log('Erasing sandbox for %s' % task_id)
    self._erase_sandbox(task_id)
    self.log('Erasing logs for %s' % task_id)
    self._collector.erase_logs(task_id)
    self.log('Erasing metadata for %s' % task_id)
    self._collector.erase_metadata(task_id)

  def garbage_collect(self, force_delete=frozenset()):
    """Garbage collect tasks on the system no longer active or in the supplied force_delete.

    Return a set of task_ids representing the tasks that were garbage collected.
    """
    active_tasks, finished_tasks = self.partition_tasks()
    retained_executors = set(iter(self.linked_executors))
    self.log('Executor sandboxes retained by Mesos:')
    if retained_executors:
      for r_e in sorted(retained_executors):
        self.log('  %s' % r_e)
    else:
      self.log('  None')
    for task_id in (active_tasks - retained_executors):
      self.log('ERROR: Active task %s had its executor sandbox pulled.' % task_id)
    gc_tasks = (finished_tasks - retained_executors) | force_delete
    for gc_task in gc_tasks:
      self._gc(gc_task)
    return gc_tasks

  @property
  def linked_executors(self):
    """Generator yielding the executor sandboxes detected on the system."""
    thermos_executor_prefix = 'thermos-'
    for executor in self._detector:
      # It's possible for just the 'latest' symlink to be present but no run directories.
      # This indicates that the task has been fully garbage collected.
      if executor.executor_id.startswith(thermos_executor_prefix) and executor.run != 'latest':
        yield executor.executor_id[len(thermos_executor_prefix):]

  def _run_gc(self, task, retain_tasks, retain_start):
    """
      Reconcile the set of tasks to retain (provided by the scheduler) with the current state of
      executors on this system. Garbage collect tasks/executors as appropriate.

      Not re-entrant! Previous executions must complete (and clear self._task_id) before this can be
      invoked.

      Potentially blocking (e.g. on I/O) in self.garbage_collect()

      Args:
        task: TaskInfo provided by the slave
        retain_tasks: mapping of task_id => ScheduleStatus, describing what the scheduler thinks is
                      running on this system
        retain_start: the time at which the retain_tasks message is effective -- this means that
                      tasks started after the retain_tasks message is effective are skipped
                      until future GC runs.
    """
    task_id = task.task_id.value
    if self._task_id is not None:
      raise RuntimeError('_run_gc() called [task_id=%s], but already running [task_id=%s]'
                         % (task_id, self._task_id))
    self._task_id = task_id
    self.log('Launching garbage collection [task_id=%s]' % task_id)
    self._start_time = retain_start
    local_gc, _, _ = self.reconcile_states(self._driver, retain_tasks)
    self.garbage_collect(local_gc)
    self.send_update(
        self._driver, task.task_id.value, mesos_pb2.TASK_FINISHED, 'Garbage collection finished.')
    self.log('Garbage collection complete [task_id=%s]' % task_id)
    self._task_id = self._start_time = None

  def run(self):
    """Main GC executor event loop.

      Periodically perform state reconciliation with the set of tasks provided
      by the slave, and garbage collect orphaned tasks on the system.
    """
    run_start = self._clock.time()
    last_gc_run = self._clock.time()

    def should_terminate():
      now = self._clock.time()
      if now > run_start + self.MAXIMUM_EXECUTOR_LIFETIME.as_(Time.SECONDS):
        return True
      return self._stop_event.is_set()

    while not should_terminate():
      try:
        _, (task, retain_tasks, retain_start) = self._gc_task_queue.popitem(0)
        last_gc_run = retain_start
        self._run_gc(task, retain_tasks, retain_start)
      except KeyError:  # no enqueued GC tasks
        pass
      if self._driver is not None:
        self.clean_orphans(self._driver)
      self._stop_event.wait(self.POLL_WAIT.as_(Time.SECONDS))

    # shutdown
    if self._driver is not None:
      try:
        prev_task_id, _ = self._gc_task_queue.popitem(0)
      except KeyError:  # no enqueued GC tasks
        pass
      else:
        self.send_update(self._driver, prev_task_id, mesos_pb2.TASK_FINISHED,
                         'Garbage collection skipped - GC executor shutting down')
        # TODO(jon) Remove this once external MESOS-243 is resolved.
        self.log('Sleeping briefly to mitigate https://issues.apache.org/jira/browse/MESOS-243')
        self._clock.sleep(self.PERSISTENCE_WAIT.as_(Time.SECONDS))

      self._driver.stop()

  """ Mesos Executor API methods follow """

  def launchTask(self, driver, task):
    """Queue a new garbage collection run, and drop any currently-enqueued runs."""
    if self._slave_id is None:
      self._slave_id = task.slave_id.value
    task_id = task.task_id.value
    self.log('launchTask() got task_id: %s' % task_id)
    if self._stop_event.is_set():
      self.log('=> Executor is shutting down - ignoring task %s' % task_id)
      self.send_update(
          self._driver, task_id, mesos_pb2.TASK_FAILED, 'GC Executor is shutting down.')
      return
    elif task_id == self._task_id:
      self.log('=> GC with task_id %s currently running - ignoring' % task_id)
      return
    elif task_id in self._gc_task_queue:
      self.log('=> Already have task_id %s queued - ignoring' % task_id)
      return
    try:
      art = thrift_deserialize(AdjustRetainedTasks(), task.data)
    except Exception as err:
      self.log('Error deserializing task: %s' % err)
      self.send_update(
          self._driver, task_id, mesos_pb2.TASK_FAILED, 'Deserialization of GC task failed')
      return
    try:
      prev_task_id, _ = self._gc_task_queue.popitem(0)
    except KeyError:  # no enqueued GC tasks - reset counter
      self._dropped_tasks.write(0)
    else:
      self.log('=> Dropping previously queued GC with task_id %s' % prev_task_id)
      self._dropped_tasks.increment()
      self.log('=> Updating scheduler')
      self.send_update(self._driver, prev_task_id, mesos_pb2.TASK_FINISHED,
                       'Garbage collection skipped - GC executor received another task')
    self.log('=> Adding %s to GC queue' % task_id)
    self._gc_task_queue[task_id] = (task, art.retainedTasks, self._clock.time())

  def killTask(self, driver, task_id):
    """Remove the specified task from the queue, if it's not yet run. Otherwise, no-op."""
    self.log('killTask() got task_id: %s' % task_id)
    task = self._gc_task_queue.pop(task_id, None)
    if task is not None:
      self.log('=> Removed %s from queued GC tasks' % task_id)
    elif task_id == self._task_id:
      self.log('=> GC with task_id %s currently running - ignoring' % task_id)
    else:
      self.log('=> Unknown task_id %s - ignoring' % task_id)

  def shutdown(self, driver):
    """Trigger the Executor to shut down as soon as the current GC run is finished."""
    self.log('shutdown() called - setting stop event')
    self._stop_event.set()
