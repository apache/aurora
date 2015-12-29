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

""" Thermos runner.

This module contains the TaskRunner, the core component of Thermos responsible for actually running
tasks. It also contains several Handlers which define the behaviour on state transitions within the
TaskRunner.

There are three "active" states in a running Thermos task:
  ACTIVE
  CLEANING
  FINALIZING

A task in ACTIVE state is running regular processes.  The moment this task succeeds or goes over its
failure limit, it then goes into CLEANING state, where it begins the staged termination of leftover
processes (with SIGTERMs).  Once all processes have terminated, the task goes into FINALIZING state,
where the processes marked with the 'final' bit run.  Once the task has gone into CLEANING state, it
has a deadline for going into terminal state.  If it doesn't make it in time (at any point, whether
in CLEANING or FINALIZING state), it is forced into terminal state through SIGKILLs of all live
processes (coordinators, shells and the full process trees rooted at the shells.)

TaskRunner.kill is implemented by forcing the task into CLEANING state and setting its finalization
deadline manually.  So in practice, we implement Task preemption by calling kill with the
finalization deadline = now + preemption wait, which gives the Task an opportunity to do graceful
shutdown.  If preemption_wait=0, it will result in immediate SIGKILLs and then transition to the
terminal state.

"""

import os
import pwd
import socket
import sys
import time
import traceback
from contextlib import contextmanager

from pystachio import Empty, Environment
from twitter.common import log
from twitter.common.dirutil import safe_mkdir
from twitter.common.quantity import Amount, Data, Time
from twitter.common.recordio import ThriftRecordReader

from apache.thermos.common.ckpt import (
    CheckpointDispatcher,
    ProcessStateHandler,
    TaskStateHandler,
    UniversalStateHandler
)
from apache.thermos.common.path import TaskPath
from apache.thermos.common.planner import TaskPlanner
from apache.thermos.config.loader import (
    ThermosConfigLoader,
    ThermosTaskValidator,
    ThermosTaskWrapper
)
from apache.thermos.config.schema import ThermosContext

from .helper import TaskRunnerHelper
from .muxer import ProcessMuxer
from .process import LoggerMode, Process

from gen.apache.thermos.ttypes import (
    ProcessState,
    ProcessStatus,
    RunnerCkpt,
    RunnerHeader,
    RunnerState,
    TaskState,
    TaskStatus
)


# TODO(wickman) Currently this is messy because of all the private access into ._runner.
# Clean this up by giving the TaskRunnerProcessHandler the components it should own, and
# create a legitimate API contract into the Runner.
class TaskRunnerProcessHandler(ProcessStateHandler):
  """
    Accesses these parts of the runner:

      | _task_processes [array set, pop]
      | _task_process_from_process_name [process name / sequence number => Process]
      | _watcher [ProcessMuxer.register, unregister]
      | _plan [add_success, add_failure, set_running]
  """

  def __init__(self, runner):
    self._runner = runner

  def on_waiting(self, process_update):
    log.debug('Process on_waiting %s' % process_update)
    self._runner._task_processes[process_update.process] = (
      self._runner._task_process_from_process_name(
        process_update.process, process_update.seq + 1))
    self._runner._watcher.register(process_update.process, process_update.seq - 1)

  def on_forked(self, process_update):
    log.debug('Process on_forked %s' % process_update)
    task_process = self._runner._task_processes[process_update.process]
    task_process.rebind(process_update.coordinator_pid, process_update.fork_time)
    self._runner._plan.set_running(process_update.process)

  def on_running(self, process_update):
    log.debug('Process on_running %s' % process_update)
    self._runner._plan.set_running(process_update.process)

  def _cleanup(self, process_update):
    if not self._runner._recovery:
      TaskRunnerHelper.kill_process(self._runner.state, process_update.process)

  def on_success(self, process_update):
    log.debug('Process on_success %s' % process_update)
    log.info('Process(%s) finished successfully [rc=%s]' % (
      process_update.process, process_update.return_code))
    self._cleanup(process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    self._runner._plan.add_success(process_update.process)

  def _on_abnormal(self, process_update):
    log.info('Process %s had an abnormal termination' % process_update.process)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)

  def on_failed(self, process_update):
    log.debug('Process on_failed %s' % process_update)
    log.info('Process(%s) failed [rc=%s]' % (process_update.process, process_update.return_code))
    self._cleanup(process_update)
    self._on_abnormal(process_update)
    self._runner._plan.add_failure(process_update.process)
    if process_update.process in self._runner._plan.failed:
      log.info('Process %s reached maximum failures, marking process run failed.' %
          process_update.process)
    else:
      log.info('Process %s under maximum failure limit, restarting.' % process_update.process)

  def on_lost(self, process_update):
    log.debug('Process on_lost %s' % process_update)
    self._cleanup(process_update)
    self._on_abnormal(process_update)
    self._runner._plan.lost(process_update.process)

  def on_killed(self, process_update):
    log.debug('Process on_killed %s' % process_update)
    self._cleanup(process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    log.debug('Process killed, marking it as a loss.')
    self._runner._plan.lost(process_update.process)


class TaskRunnerTaskHandler(TaskStateHandler):
  """
    Accesses these parts of the runner:
      _plan [set to regular_plan or finalizing_plan]
      _recovery [boolean, whether or not to side-effect]
      _pathspec [path creation]
      _task [ThermosTask]
      _set_finalization_start
      _kill
  """

  def __init__(self, runner):
    self._runner = runner
    self._pathspec = self._runner._pathspec

  def on_active(self, task_update):
    log.debug('Task on_active(%s)' % task_update)
    self._runner._plan = self._runner._regular_plan
    if self._runner._recovery:
      return
    TaskRunnerHelper.initialize_task(self._pathspec,
        ThermosTaskWrapper(self._runner._task).to_json())

  def on_cleaning(self, task_update):
    log.debug('Task on_cleaning(%s)' % task_update)
    self._runner._finalization_start = task_update.timestamp_ms / 1000.0
    self._runner._terminate_plan(self._runner._regular_plan)

  def on_finalizing(self, task_update):
    log.debug('Task on_finalizing(%s)' % task_update)
    if not self._runner._recovery:
      self._runner._kill()
    self._runner._plan = self._runner._finalizing_plan
    if self._runner._finalization_start is None:
      self._runner._finalization_start = task_update.timestamp_ms / 1000.0

  def on_killed(self, task_update):
    log.debug('Task on_killed(%s)' % task_update)
    self._cleanup()

  def on_success(self, task_update):
    log.debug('Task on_success(%s)' % task_update)
    self._cleanup()
    log.info('Task succeeded.')

  def on_failed(self, task_update):
    log.debug('Task on_failed(%s)' % task_update)
    self._cleanup()

  def on_lost(self, task_update):
    log.debug('Task on_lost(%s)' % task_update)
    self._cleanup()

  def _cleanup(self):
    if not self._runner._recovery:
      self._runner._kill()
      TaskRunnerHelper.finalize_task(self._pathspec)


class TaskRunnerUniversalHandler(UniversalStateHandler):
  """
    Universal handler to checkpoint every process and task transition of the runner.

    Accesses these parts of the runner:
      _ckpt_write
  """

  def __init__(self, runner):
    self._runner = runner

  def _checkpoint(self, record):
    self._runner._ckpt_write(record)

  def on_process_transition(self, state, process_update):
    log.debug('_on_process_transition: %s' % process_update)
    self._checkpoint(RunnerCkpt(process_status=process_update))

  def on_task_transition(self, state, task_update):
    log.debug('_on_task_transition: %s' % task_update)
    self._checkpoint(RunnerCkpt(task_status=task_update))

  def on_initialization(self, header):
    log.debug('_on_initialization: %s' % header)
    ThermosTaskValidator.assert_valid_task(self._runner.task)
    ThermosTaskValidator.assert_valid_ports(self._runner.task, header.ports)
    self._checkpoint(RunnerCkpt(runner_header=header))


class TaskRunnerStage(object):
  """
    A stage of the task runner pipeline.
  """
  MAX_ITERATION_WAIT = Amount(1, Time.SECONDS)

  def __init__(self, runner):
    self.runner = runner
    self.clock = runner._clock

  def run(self):
    """
      Perform any work necessary at this stage of the task.

      If there is no more work to be done, return None. [This will invoke a state transition.]

      If there is still work to be done, return the number of seconds from now in which you'd like
      to be called to re-run the plan.
    """
    return None

  def transition_to(self):
    """
      The stage to which we should transition.
    """
    raise NotImplementedError


class TaskRunnerStage_ACTIVE(TaskRunnerStage):  # noqa
  """
    Run the regular plan (i.e. normal, non-finalizing processes.)
  """
  MAX_ITERATION_WAIT = Amount(15, Time.SECONDS)
  MIN_ITERATION_WAIT = Amount(1, Time.SECONDS)

  def __init__(self, runner):
    super(TaskRunnerStage_ACTIVE, self).__init__(runner)

  def run(self):
    launched = self.runner._run_plan(self.runner._regular_plan)

    # Have we terminated?
    terminal_state = None
    if self.runner._regular_plan.is_complete():
      log.info('Regular plan complete.')
      terminal_state = TaskState.SUCCESS if self.runner.is_healthy() else TaskState.FAILED
    elif not self.runner.is_healthy():
      log.error('Regular plan unhealthy!')
      terminal_state = TaskState.FAILED

    if terminal_state:
      # No more work to do
      return None
    elif launched > 0:
      # We want to run ASAP after updates have been collected
      return max(self.MIN_ITERATION_WAIT.as_(Time.SECONDS), self.runner._regular_plan.min_wait())
    else:
      # We want to run as soon as something is available to run or after a prescribed timeout.
      return min(self.MAX_ITERATION_WAIT.as_(Time.SECONDS), self.runner._regular_plan.min_wait())

  def transition_to(self):
    return TaskState.CLEANING


class TaskRunnerStage_CLEANING(TaskRunnerStage):  # noqa
  """
    Start the cleanup of the regular plan (e.g. if it failed.)  On ACTIVE -> CLEANING,
    we send SIGTERMs to all still-running processes.  We wait at most finalization_wait
    for all processes to complete before SIGKILLs are sent.  If everything exits cleanly
    prior to that point in time, we transition to FINALIZING, which kicks into gear
    the finalization schedule (if any.)
  """

  def run(self):
    log.debug('TaskRunnerStage[CLEANING]: Finalization remaining: %s' %
        self.runner._finalization_remaining())
    if self.runner._finalization_remaining() > 0 and self.runner.has_running_processes():
      return min(self.runner._finalization_remaining(), self.MAX_ITERATION_WAIT.as_(Time.SECONDS))

  def transition_to(self):
    if self.runner._finalization_remaining() <= 0:
      log.info('Exceeded finalization wait, skipping finalization.')
      return self.runner.terminal_state()
    return TaskState.FINALIZING


class TaskRunnerStage_FINALIZING(TaskRunnerStage):  # noqa
  """
    Run the finalizing plan, specifically the plan of tasks with the 'final'
    bit marked (e.g. log savers, checkpointers and the like.)  Anything in this
    plan will be SIGKILLed if we go over the finalization_wait.
  """

  def run(self):
    self.runner._run_plan(self.runner._finalizing_plan)
    log.debug('TaskRunnerStage[FINALIZING]: Finalization remaining: %s' %
        self.runner._finalization_remaining())
    if self.runner.deadlocked(self.runner._finalizing_plan):
      log.warning('Finalizing plan deadlocked.')
      return None
    if self.runner._finalization_remaining() > 0 and not self.runner._finalizing_plan.is_complete():
      return min(self.runner._finalization_remaining(), self.MAX_ITERATION_WAIT.as_(Time.SECONDS))

  def transition_to(self):
    if self.runner._finalization_remaining() <= 0:
      log.info('Exceeded finalization wait, terminating finalization.')
    return self.runner.terminal_state()


class TaskRunner(object):
  """
    Run a ThermosTask.

    This class encapsulates the core logic to run and control the state of a Thermos task.
    Typically, it will be instantiated directly to control a new task, but a TaskRunner can also be
    synthesised from an existing task's checkpoint root
  """
  class Error(Exception): pass
  class InternalError(Error): pass
  class InvalidTask(Error): pass
  class PermissionError(Error): pass
  class StateError(Error): pass

  # Maximum amount of time we spend waiting for new updates from the checkpoint streams
  # before doing housecleaning (checking for LOST tasks, dead PIDs.)
  MAX_ITERATION_TIME = Amount(10, Time.SECONDS)

  # Minimum amount of time we wait between polls for updates on coordinator checkpoints.
  COORDINATOR_INTERVAL_SLEEP = Amount(1, Time.SECONDS)

  # Amount of time we're willing to wait after forking before we expect the runner to have
  # exec'ed the child process.
  LOST_TIMEOUT = Amount(60, Time.SECONDS)

  # Active task stages
  STAGES = {
    TaskState.ACTIVE: TaskRunnerStage_ACTIVE,
    TaskState.CLEANING: TaskRunnerStage_CLEANING,
    TaskState.FINALIZING: TaskRunnerStage_FINALIZING
  }

  @classmethod
  def get(cls, task_id, checkpoint_root):
    """
      Get a TaskRunner bound to the task_id in checkpoint_root.
    """
    path = TaskPath(root=checkpoint_root, task_id=task_id, state='active')
    task_json = path.getpath('task_path')
    task_checkpoint = path.getpath('runner_checkpoint')
    if not os.path.exists(task_json):
      return None
    task = ThermosConfigLoader.load_json(task_json)
    if task is None:
      return None
    if len(task.tasks()) == 0:
      return None
    try:
      checkpoint = CheckpointDispatcher.from_file(task_checkpoint)
      if checkpoint is None or checkpoint.header is None:
        return None
      return cls(task.tasks()[0].task(), checkpoint_root, checkpoint.header.sandbox,
                 log_dir=checkpoint.header.log_dir, task_id=task_id,
                 portmap=checkpoint.header.ports, hostname=checkpoint.header.hostname)
    except Exception as e:
      log.error('Failed to reconstitute checkpoint in TaskRunner.get: %s' % e, exc_info=True)
      return None

  def __init__(self, task, checkpoint_root, sandbox, log_dir=None,
               task_id=None, portmap=None, user=None, chroot=False, clock=time,
               universal_handler=None, planner_class=TaskPlanner, hostname=None,
               process_logger_mode=None, rotate_log_size_mb=None, rotate_log_backups=None,
               preserve_env=False):
    """
      required:
        task (config.Task) = the task to run
        checkpoint_root (path) = the checkpoint root
        sandbox (path) = the sandbox in which the path will be run
                         [if None, cwd will be assumed, but garbage collection will be
                          disabled for this task.]

      optional:
        log_dir (string)  = directory to house stdout/stderr logs. If not specified, logs will be
                            written into the sandbox directory under .logs/
        task_id (string)  = bind to this task id.  if not specified, will synthesize an id based
                            upon task.name()
        portmap (dict)    = a map (string => integer) from name to port, e.g. { 'http': 80 }
        user (string)     = the user to run the task as.  if not current user, requires setuid
                            privileges.
        chroot (boolean)  = whether or not to chroot into the sandbox prior to exec.
        clock (time interface) = the clock to use throughout
        universal_handler = checkpoint record handler (only used for testing)
        planner_class (TaskPlanner class) = TaskPlanner class to use for constructing the task
                            planning policy.
        process_logger_mode (string) = The type of logger to use for all processes.
        rotate_log_size_mb (integer) = The maximum size of the rotated stdout/stderr logs in MiB.
        rotate_log_backups (integer) = The maximum number of rotated stdout/stderr log backups.
        preserve_env (boolean) = whether or not env variables for the runner should be in the
                                 env for the task being run
    """
    if not issubclass(planner_class, TaskPlanner):
      raise TypeError('planner_class must be a TaskPlanner.')
    self._clock = clock
    launch_time = self._clock.time()
    launch_time_ms = '%06d' % int((launch_time - int(launch_time)) * (10 ** 6))
    if not task_id:
      self._task_id = '%s-%s.%s' % (task.name(),
                                    time.strftime('%Y%m%d-%H%M%S', time.localtime(launch_time)),
                                    launch_time_ms)
    else:
      self._task_id = task_id
    current_user = TaskRunnerHelper.get_actual_user()
    self._user = user or current_user
    # TODO(wickman) This should be delegated to the ProcessPlatform / Helper
    if self._user != current_user:
      if os.geteuid() != 0:
        raise ValueError('task specifies user as %s, but %s does not have setuid permission!' % (
          self._user, current_user))
    self._portmap = portmap or {}
    self._launch_time = launch_time
    self._log_dir = log_dir or os.path.join(sandbox, '.logs')
    self._process_logger_mode = process_logger_mode
    self._rotate_log_size_mb = rotate_log_size_mb
    self._rotate_log_backups = rotate_log_backups
    self._pathspec = TaskPath(root=checkpoint_root, task_id=self._task_id, log_dir=self._log_dir)
    self._hostname = hostname or socket.gethostname()
    try:
      ThermosTaskValidator.assert_valid_task(task)
      ThermosTaskValidator.assert_valid_ports(task, self._portmap)
    except ThermosTaskValidator.InvalidTaskError as e:
      raise self.InvalidTask('Invalid task: %s' % e)
    context = ThermosContext(
        task_id=self._task_id,
        ports=self._portmap,
        user=self._user)
    self._task, uninterp = (task % Environment(thermos=context)).interpolate()
    if len(uninterp) > 0:
      raise self.InvalidTask('Failed to interpolate task, missing: %s' %
          ', '.join(str(ref) for ref in uninterp))
    try:
      ThermosTaskValidator.assert_same_task(self._pathspec, self._task)
    except ThermosTaskValidator.InvalidTaskError as e:
      raise self.InvalidTask('Invalid task: %s' % e)
    self._plan = None  # plan currently being executed (updated by Handlers)
    self._regular_plan = planner_class(self._task, clock=clock,
        process_filter=lambda proc: proc.final().get() is False)
    self._finalizing_plan = planner_class(self._task, clock=clock,
        process_filter=lambda proc: proc.final().get() is True)
    self._chroot = chroot
    self._sandbox = sandbox
    self._terminal_state = None
    self._ckpt = None
    self._process_map = dict((p.name().get(), p) for p in self._task.processes())
    self._task_processes = {}
    self._stages = dict((state, stage(self)) for state, stage in self.STAGES.items())
    self._finalization_start = None
    self._preemption_deadline = None
    self._watcher = ProcessMuxer(self._pathspec)
    self._state = RunnerState(processes={})
    self._preserve_env = preserve_env

    # create runner state
    universal_handler = universal_handler or TaskRunnerUniversalHandler
    self._dispatcher = CheckpointDispatcher()
    self._dispatcher.register_handler(universal_handler(self))
    self._dispatcher.register_handler(TaskRunnerProcessHandler(self))
    self._dispatcher.register_handler(TaskRunnerTaskHandler(self))

    # recover checkpointed runner state and update plan
    self._recovery = True
    self._replay_runner_ckpt()

  @property
  def task(self):
    return self._task

  @property
  def task_id(self):
    return self._task_id

  @property
  def state(self):
    return self._state

  @property
  def processes(self):
    return self._task_processes

  def task_state(self):
    return self._state.statuses[-1].state if self._state.statuses else TaskState.ACTIVE

  def close_ckpt(self):
    """Force close the checkpoint stream.  This is necessary for runners terminated through
       exception propagation."""
    log.debug('Closing the checkpoint stream.')
    self._ckpt.close()

  @contextmanager
  def control(self, force=False):
    """
      Bind to the checkpoint associated with this task, position to the end of the log if
      it exists, or create it if it doesn't.  Fails if we cannot get "leadership" i.e. a
      file lock on the checkpoint stream.
    """
    if self.is_terminal():
      raise self.StateError('Cannot take control of a task in terminal state.')
    if self._sandbox:
      safe_mkdir(self._sandbox)
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    try:
      self._ckpt = TaskRunnerHelper.open_checkpoint(ckpt_file, force=force, state=self._state)
    except TaskRunnerHelper.PermissionError:
      raise self.PermissionError('Unable to open checkpoint %s' % ckpt_file)
    log.debug('Flipping recovery mode off.')
    self._recovery = False
    self._set_task_status(self.task_state())
    self._resume_task()
    try:
      yield
    except Exception as e:
      log.error('Caught exception in self.control(): %s' % e)
      log.error('  %s' % traceback.format_exc())
    self._ckpt.close()

  def _resume_task(self):
    assert self._ckpt is not None
    unapplied_updates = self._replay_process_ckpts()
    if self.is_terminal():
      raise self.StateError('Cannot resume terminal task.')
    self._initialize_ckpt_header()
    self._replay(unapplied_updates)

  def _ckpt_write(self, record):
    """
      Write to the checkpoint stream if we're not in recovery mode.
    """
    if not self._recovery:
      self._ckpt.write(record)

  def _replay(self, checkpoints):
    """
      Replay a sequence of RunnerCkpts.
    """
    for checkpoint in checkpoints:
      self._dispatcher.dispatch(self._state, checkpoint)

  def _replay_runner_ckpt(self):
    """
      Replay the checkpoint stream associated with this task.
    """
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    if os.path.exists(ckpt_file):
      with open(ckpt_file, 'r') as fp:
        ckpt_recover = ThriftRecordReader(fp, RunnerCkpt)
        for record in ckpt_recover:
          log.debug('Replaying runner checkpoint record: %s' % record)
          self._dispatcher.dispatch(self._state, record, recovery=True)

  def _replay_process_ckpts(self):
    """
      Replay the unmutating process checkpoints.  Return the unapplied process updates that
      would mutate the runner checkpoint stream.
    """
    process_updates = self._watcher.select()
    unapplied_process_updates = []
    for process_update in process_updates:
      if self._dispatcher.would_update(self._state, process_update):
        unapplied_process_updates.append(process_update)
      else:
        self._dispatcher.dispatch(self._state, process_update, recovery=True)
    return unapplied_process_updates

  def _initialize_ckpt_header(self):
    """
      Initializes the RunnerHeader for this checkpoint stream if it has not already
      been constructed.
    """
    if self._state.header is None:
      try:
        uid = pwd.getpwnam(self._user).pw_uid
      except KeyError:
        # This will cause failures downstream, but they will at least be correctly
        # reflected in the process state.
        log.error('Unknown user %s.' % self._user)
        uid = None

      header = RunnerHeader(
          task_id=self._task_id,
          launch_time_ms=int(self._launch_time * 1000),
          sandbox=self._sandbox,
          log_dir=self._log_dir,
          hostname=self._hostname,
          user=self._user,
          uid=uid,
          ports=self._portmap)
      runner_ckpt = RunnerCkpt(runner_header=header)
      self._dispatcher.dispatch(self._state, runner_ckpt)

  def _set_task_status(self, state):
    update = TaskStatus(state=state, timestamp_ms=int(self._clock.time() * 1000),
                        runner_pid=os.getpid(), runner_uid=os.getuid())
    runner_ckpt = RunnerCkpt(task_status=update)
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _finalization_remaining(self):
    # If a preemption deadline has been set, use that.
    if self._preemption_deadline:
      return max(0, self._preemption_deadline - self._clock.time())

    # Otherwise, use the finalization wait provided in the configuration.
    finalization_allocation = self.task.finalization_wait().get()
    if self._finalization_start is None:
      return sys.float_info.max
    else:
      waited = max(0, self._clock.time() - self._finalization_start)
      return max(0, finalization_allocation - waited)

  def _set_process_status(self, process_name, process_state, **kw):
    if 'sequence_number' in kw:
      sequence_number = kw.pop('sequence_number')
      log.debug('_set_process_status(%s <= %s, seq=%s[force])' % (process_name,
        ProcessState._VALUES_TO_NAMES.get(process_state), sequence_number))
    else:
      current_run = self._current_process_run(process_name)
      if not current_run:
        assert process_state == ProcessState.WAITING
        sequence_number = 0
      else:
        sequence_number = current_run.seq + 1
      log.debug('_set_process_status(%s <= %s, seq=%s[auto])' % (process_name,
        ProcessState._VALUES_TO_NAMES.get(process_state), sequence_number))
    runner_ckpt = RunnerCkpt(process_status=ProcessStatus(
        process=process_name, state=process_state, seq=sequence_number, **kw))
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _task_process_from_process_name(self, process_name, sequence_number):
    """
      Construct a Process() object from a process_name, populated with its
      correct run number and fully interpolated commandline.
    """
    run_number = len(self.state.processes[process_name]) - 1
    pathspec = self._pathspec.given(process=process_name, run=run_number)
    process = self._process_map.get(process_name)
    if process is None:
      raise self.InternalError('FATAL: Could not find process: %s' % process_name)
    def close_ckpt_and_fork():
      pid = os.fork()
      if pid == 0 and self._ckpt is not None:
        self._ckpt.close()
      return pid

    logger_mode, rotate_log_size, rotate_log_backups = self._build_process_logger_args(process)

    return Process(
      process.name().get(),
      process.cmdline().get(),
      sequence_number,
      pathspec,
      self._sandbox,
      self._user,
      chroot=self._chroot,
      fork=close_ckpt_and_fork,
      logger_mode=logger_mode,
      rotate_log_size=rotate_log_size,
      rotate_log_backups=rotate_log_backups,
      preserve_env=self._preserve_env)

  def _build_process_logger_args(self, process):
    """
      Build the appropriate logging configuration based on flags + process
      configuration settings.

      If no configuration (neither flags nor process config), default to
      "standard" mode.
    """
    logger = process.logger()
    if logger is Empty:
      if self._process_logger_mode:
        return (
          self._process_logger_mode,
          Amount(self._rotate_log_size_mb, Data.MB),
          self._rotate_log_backups
        )
      else:
        return LoggerMode.STANDARD, None, None
    else:
      mode = logger.mode().get()
      if mode == LoggerMode.ROTATE:
        rotate = logger.rotate()
        return mode, Amount(rotate.log_size().get(), Data.BYTES), rotate.backups().get()
      else:
        return mode, None, None

  def deadlocked(self, plan=None):
    """Check whether a plan is deadlocked, i.e. there are no running/runnable processes, and the
    plan is not complete."""
    plan = plan or self._regular_plan
    now = self._clock.time()
    running = list(plan.running)
    runnable = list(plan.runnable_at(now))
    waiting = list(plan.waiting_at(now))
    log.debug('running:%d runnable:%d waiting:%d complete:%s' % (
      len(running), len(runnable), len(waiting), plan.is_complete()))
    return len(running + runnable + waiting) == 0 and not plan.is_complete()

  def is_healthy(self):
    """Check whether the TaskRunner is healthy. A healthy TaskRunner is not deadlocked and has not
    reached its max_failures count."""
    max_failures = self._task.max_failures().get()
    deadlocked = self.deadlocked()
    under_failure_limit = max_failures == 0 or len(self._regular_plan.failed) < max_failures
    log.debug('max_failures:%d failed:%d under_failure_limit:%s deadlocked:%s ==> health:%s' % (
      max_failures, len(self._regular_plan.failed), under_failure_limit, deadlocked,
      not deadlocked and under_failure_limit))
    return not deadlocked and under_failure_limit

  def _current_process_run(self, process_name):
    if process_name not in self._state.processes or len(self._state.processes[process_name]) == 0:
      return None
    return self._state.processes[process_name][-1]

  def is_process_lost(self, process_name):
    """Determine whether or not we should mark a task as LOST and do so if necessary."""
    current_run = self._current_process_run(process_name)
    if not current_run:
      raise self.InternalError('No current_run for process %s!' % process_name)

    def forked_but_never_came_up():
      return current_run.state == ProcessState.FORKED and (
        self._clock.time() - current_run.fork_time > self.LOST_TIMEOUT.as_(Time.SECONDS))

    def running_but_coordinator_died():
      if current_run.state != ProcessState.RUNNING:
        return False
      coordinator_pid, _, _ = TaskRunnerHelper.scan_process(self.state, process_name)
      if coordinator_pid is not None:
        return False
      elif self._watcher.has_data(process_name):
        return False
      return True

    if forked_but_never_came_up() or running_but_coordinator_died():
      log.info('Detected a LOST task: %s' % current_run)
      log.debug('  forked_but_never_came_up: %s' % forked_but_never_came_up())
      log.debug('  running_but_coordinator_died: %s' % running_but_coordinator_died())
      return True

    return False

  def _run_plan(self, plan):
    log.debug('Schedule pass:')

    running = list(plan.running)
    log.debug('running: %s' % ' '.join(plan.running))
    log.debug('finished: %s' % ' '.join(plan.finished))

    launched = []
    for process_name in plan.running:
      if self.is_process_lost(process_name):
        self._set_process_status(process_name, ProcessState.LOST)

    now = self._clock.time()
    runnable = list(plan.runnable_at(now))
    waiting = list(plan.waiting_at(now))
    log.debug('runnable: %s' % ' '.join(runnable))
    log.debug('waiting: %s' % ' '.join(
        '%s[T-%.1fs]' % (process, plan.get_wait(process)) for process in waiting))

    def pick_processes(process_list):
      if self._task.max_concurrency().get() == 0:
        return process_list
      num_to_pick = max(self._task.max_concurrency().get() - len(running), 0)
      return process_list[:num_to_pick]

    for process_name in pick_processes(runnable):
      tp = self._task_processes.get(process_name)
      if tp:
        current_run = self._current_process_run(process_name)
        assert current_run.state == ProcessState.WAITING
      else:
        self._set_process_status(process_name, ProcessState.WAITING)
        tp = self._task_processes[process_name]
      log.info('Forking Process(%s)' % process_name)
      try:
        tp.start()
        launched.append(tp)
      except Process.Error as e:
        log.error('Failed to launch process: %s' % e)
        self._set_process_status(process_name, ProcessState.FAILED)

    return len(launched) > 0

  def _terminate_plan(self, plan):
    for process in plan.running:
      last_run = self._current_process_run(process)
      if last_run and last_run.state in (ProcessState.FORKED, ProcessState.RUNNING):
        TaskRunnerHelper.terminate_process(self.state, process)

  def has_running_processes(self):
    """
      Returns True if any processes associated with this task have active pids.
    """
    process_tree = TaskRunnerHelper.scan_tree(self.state)
    return any(any(process_set) for process_set in process_tree.values())

  def has_active_processes(self):
    """
      Returns True if any processes are in non-terminal states.
    """
    return any(not TaskRunnerHelper.is_process_terminal(run.state) for run in
        filter(None, (self._current_process_run(process) for process in self.state.processes)))

  def collect_updates(self, timeout=None):
    """
      Collects and applies updates from process checkpoint streams.  Returns the number
      of applied process checkpoints.
    """
    if not self.has_active_processes():
      return 0

    sleep_interval = self.COORDINATOR_INTERVAL_SLEEP.as_(Time.SECONDS)
    total_time = 0.0

    while True:
      process_updates = self._watcher.select()
      for process_update in process_updates:
        self._dispatcher.dispatch(self._state, process_update, self._recovery)
      if process_updates:
        return len(process_updates)
      if timeout is not None and total_time >= timeout:
        return 0
      total_time += sleep_interval
      self._clock.sleep(sleep_interval)

  def is_terminal(self):
    return TaskRunnerHelper.is_task_terminal(self.task_state())

  def terminal_state(self):
    if self._terminal_state:
      log.debug('Forced terminal state: %s' %
          TaskState._VALUES_TO_NAMES.get(self._terminal_state, 'UNKNOWN'))
      return self._terminal_state
    else:
      return TaskState.SUCCESS if self.is_healthy() else TaskState.FAILED

  def run(self, force=False):
    """
      Entrypoint to runner. Assume control of checkpoint stream, and execute TaskRunnerStages
      until runner is terminal.
    """
    if self.is_terminal():
      return
    with self.control(force):
      self._run()

  def _run(self):
    while not self.is_terminal():
      start = self._clock.time()
      # step 1: execute stage corresponding to the state we're currently in
      runner = self._stages[self.task_state()]
      iteration_wait = runner.run()
      if iteration_wait is None:
        log.debug('Run loop: No more work to be done in state %s' %
            TaskState._VALUES_TO_NAMES.get(self.task_state(), 'UNKNOWN'))
        self._set_task_status(runner.transition_to())
        continue
      log.debug('Run loop: Work to be done within %.1fs' % iteration_wait)
      # step 2: check child process checkpoint streams for updates
      if not self.collect_updates(iteration_wait):
        # If we don't collect any updates, at least 'touch' the checkpoint stream
        # so as to prevent garbage collection.
        elapsed = self._clock.time() - start
        if elapsed < iteration_wait:
          log.debug('Update collection only took %.1fs, idling %.1fs' % (
              elapsed, iteration_wait - elapsed))
          self._clock.sleep(iteration_wait - elapsed)
        log.debug('Run loop: No updates collected, touching checkpoint.')
        os.utime(self._pathspec.getpath('runner_checkpoint'), None)
      # step 3: reap any zombie child processes
      TaskRunnerHelper.reap_children()

  def kill(self, force=False, terminal_status=TaskState.KILLED,
           preemption_wait=Amount(1, Time.MINUTES)):
    """
      Kill all processes associated with this task and set task/process states as terminal_status
      (defaults to KILLED)
    """
    log.debug('Runner issued kill: force:%s, preemption_wait:%s' % (
      force, preemption_wait))
    assert terminal_status in (TaskState.KILLED, TaskState.LOST)
    self._preemption_deadline = self._clock.time() + preemption_wait.as_(Time.SECONDS)
    with self.control(force):
      if self.is_terminal():
        log.warning('Task is not in ACTIVE state, cannot issue kill.')
        return
      self._terminal_state = terminal_status
      if self.task_state() == TaskState.ACTIVE:
        self._set_task_status(TaskState.CLEANING)
      self._run()

  def lose(self, force=False):
    """
      Mark a task as LOST and kill any straggling processes.
    """
    self.kill(force, preemption_wait=Amount(0, Time.SECONDS), terminal_status=TaskState.LOST)

  def _kill(self):
    processes = TaskRunnerHelper.scan_tree(self._state)
    for process, pid_tuple in processes.items():
      current_run = self._current_process_run(process)
      coordinator_pid, pid, tree = pid_tuple
      if TaskRunnerHelper.is_process_terminal(current_run.state):
        if coordinator_pid or pid or tree:
          log.warning('Terminal process (%s) still has running pids:' % process)
          log.warning('  coordinator_pid: %s' % coordinator_pid)
          log.warning('              pid: %s' % pid)
          log.warning('             tree: %s' % tree)
        TaskRunnerHelper.kill_process(self.state, process)
      else:
        if coordinator_pid or pid or tree:
          log.info('Transitioning %s to KILLED' % process)
          self._set_process_status(process, ProcessState.KILLED,
            stop_time=self._clock.time(), return_code=-1)
        else:
          log.info('Transitioning %s to LOST' % process)
          if current_run.state != ProcessState.WAITING:
            self._set_process_status(process, ProcessState.LOST)
