import copy
import errno
import getpass
import os
import random
import socket
import signal
import threading
import time

from twitter.common import log
from twitter.common.dirutil import safe_mkdir
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from twitter.thermos.base.helper    import Helper
from twitter.thermos.base.path      import TaskPath
from twitter.thermos.base.ckpt      import TaskCkptDispatcher
from twitter.thermos.runner.planner import Planner
from twitter.thermos.runner.process import Process
from twitter.thermos.runner.muxer   import ProcessMuxer
from twitter.thermos.runner.ports   import EphemeralPortAllocator

from twitter.tcl.scheduler import Scheduler
from gen.twitter.tcl.ttypes import ThermosTask
from gen.twitter.thermos.ttypes import (
  ProcessRunState,
  ProcessState,
  TaskAllocatedPort,
  TaskRunnerCkpt,
  TaskRunnerHeader,
  TaskRunState,
  TaskRunnerState,
  TaskRunStateUpdate,
  TaskState,
  TaskStateUpdate,
)

from thrift.TSerialization import serialize as thrift_serialize
from thrift.TSerialization import deserialize as thrift_deserialize

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

# This is a no-op annotation, just for the sake of readability in the code for
# annotating state transitions that themselves cause checkpoint log writes.
def state_mutating(f):
  return f

class TaskRunnerHelper(object):
  @staticmethod
  def scheduler_from_task(task):
    log.debug('Constructing scheduler from %s' % task)
    scheduler = Scheduler()
    for process in task.processes:
      log.debug('   - adding process %s' % process.name)
      scheduler.add(process.name)
    for execution_dependency in task.before_constraints:
      scheduler.run_before(execution_dependency.first, execution_dependency.second)
      log.debug('   - adding constraint %s < %s' % (execution_dependency.first,
          execution_dependency.second))
    for process_set in task.together_constraints:
      scheduler.run_set_together(process_set.processes)
      log.debug('   - adding constraint %s' % ' = '.join(map(str, process_set.processes)))
    return scheduler

  @staticmethod
  def thriftify_task(thermos_task):
    """
      Perform an encode/decode on a ThermosTask.  This is necessary because
      Thrift serialization is a one-way lossy function.  (However, further
      ser/der cycles appear to be consistent.)
    """
    return thrift_deserialize(ThermosTask(), thrift_serialize(thermos_task))

  @staticmethod
  def dump_state(state, filename):
    with open(filename, 'w') as fp:
      fp.write(thrift_serialize(state))

  @staticmethod
  def read_state(filename):
    with open(filename, 'r') as fp:
      return thrift_deserialize(TaskRunnerState(), fp.read())

  @staticmethod
  def get_actual_user():
    import pwd
    try:
      pwd_entry = pwd.getpwuid(os.getuid())
    except KeyError:
      return getpass.getuser()
    return pwd_entry[0]

class TaskRunner(object):
  """
    Run a ThermosTask.
  """

  class InternalError(Exception): pass

  # Maximum drift between when the system says a task was forked and when we checkpointed
  # its fork_time (used as a heuristic to determine a forked task is really ours instead of
  # a task with coincidentally the same PID but just wrapped around.)
  MAX_START_TIME_DRIFT = Amount(10, Time.SECONDS)

  # Wait time between iterations.
  MIN_ITERATION_TIME = Amount(0.1, Time.SECONDS)

  # Maximum amount of time we spend waiting for new updates from the checkpoint streams
  # before doing housecleaning (checking for LOST tasks, dead PIDs.)
  MAX_ITERATION_TIME = Amount(10, Time.SECONDS)

  # Amount of time we're willing to wait after forking before we expect the runner to have
  # exec'ed the child process.
  LOST_TIMEOUT = Amount(60, Time.SECONDS)

  def __init__(self, task, sandbox, root_dir, task_id, user=None, chroot=False):
    """
      required:
        task (ThermosTask) = the task to run
        sandbox (path) = sandbox in which to run all processes (pathname)
        root_dir (path) = directory to store all log/ckpt data (pathname)
        task_id (str) = uid assigned to the task, used to disambiguate checkpoints

      optional:
        user (str) = the user to run the job as.  if specified, you must have setuid privileges.
        chroot (bool) = whether or not to run chrooted to the sandbox (default False)
    """
    self._task = copy.deepcopy(task)
    self._task_processes = {}
    self._task_id = task_id
    self._watcher = ProcessMuxer()
    self._pause_event = threading.Event()
    self._control = threading.Lock()
    if self._task.replica_id is None:
      raise ValueError("Task must have a replica_id!")
    self._pathspec = TaskPath(root = root_dir, task_id = task_id)
    self._recovery = True  # set in recovery mode
    self._port_allocator = EphemeralPortAllocator()
    self._ps = ProcessProviderFactory.get()
    self._user = user
    self._chroot = chroot

    # create scheduler from process task
    scheduler = TaskRunnerHelper.scheduler_from_task(task)

    # create planner from scheduler
    self._planner = Planner(scheduler)

    # set up sandbox for running process
    self._sandbox = sandbox
    safe_mkdir(self._sandbox)

    # create runner state
    self._state      = TaskRunnerState(processes = {})
    self._dispatcher = TaskCkptDispatcher()
    self._register_handlers()

    # recover checkpointed state and update plan
    self._read_ckpt()

    # if the state is active, open the checkpoint log and multiplex outstanding
    # fractional checkpoint streams.
    if self._state.state in (TaskState.ACTIVE, None):
      self._initialize_ckpt()
      self._update_runner_ckpt_to_high_watermarks()
      self._initialize_processes()
      self._run_task_state_machine()

  def state(self):
    return self._state

  def is_paused(self):
    return self._pause_event.is_set()

  def pause(self):
    """
      Signal the run-loop to stop and return once it's exited.
    """
    self._pause_event.set()
    with self._control:
      return True

  def unpause(self):
    """
      Unpause the runner.  Must call run() method following this for the event loop
      to start back up.
    """
    self._pause_event.unset()

  def _read_ckpt(self):
    """
      If the runner checkpoint associated with this task exists, replay the state.
    """
    ckpt_file = self._pathspec.getpath('runner_checkpoint')

    # Replay messages in the checkpoint
    if os.path.exists(ckpt_file):
      fp = open(ckpt_file, "r")
      ckpt_recover = ThriftRecordReader(fp, TaskRunnerCkpt)
      for record in ckpt_recover:
        self._dispatcher.update_runner_state(self._state, record, recovery=self._recovery)
      ckpt_recover.close()

  def _initialize_ckpt(self):
    """
      Bind to the checkpoint associated with this task, position to the end of the log if
      it exists, or create it if it doesn't.
    """
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    fp = Helper.safe_create_file(ckpt_file, "a")
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    self._ckpt = ckpt

  def _ckpt_write(self, record):
    """
      Write to the checkpoint if we're not in recovery mode.
    """
    if not self._recovery:
      self._ckpt.write(record)

  @state_mutating
  def _initialize_processes(self):
    if self._state.header is None:
      update = TaskRunnerHeader(
        task_id=self._task_id, launch_time=long(time.time()),
        sandbox=self._sandbox, hostname=socket.gethostname(),
        user=self._user or TaskRunnerHelper.get_actual_user())
      runner_ckpt = TaskRunnerCkpt(runner_header=update)
      self._dispatcher.update_runner_state(self._state, runner_ckpt)
      # change this if we add dispatches for non-process updates
      self._ckpt_write(runner_ckpt)
      self._set_task_state(TaskState.ACTIVE)

  @state_mutating
  def _run_task_state_machine(self):
    """
      Run the task state machine and indicate whether a terminal state was reached.
    """
    if self._is_task_failed():
      log.info('Setting task state to FAILED')
      self._set_task_state(TaskState.FAILED)
      self._kill()
      self.cleanup()
      return True

    if self._planner.is_complete():
      log.info('Setting task state to SUCCESS')
      self._set_task_state(TaskState.SUCCESS)
      self.cleanup()
      return True

    return False

  @state_mutating
  def _set_task_state(self, state):
    update = TaskStateUpdate(state = state)
    runner_ckpt = TaskRunnerCkpt(state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery):
      self._ckpt_write(runner_ckpt)

  @state_mutating
  def _set_process_history_state(self, process, state):
    update = TaskRunStateUpdate(process = process, state = state)
    runner_ckpt = TaskRunnerCkpt(history_state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery):
      self._ckpt_write(runner_ckpt)

  # process transitions for the state machine
  @state_mutating
  def _on_everything(self, process_update):
    self._ckpt_write(TaskRunnerCkpt(process_state = process_update))

  @state_mutating
  # TODO(wickman)  Port bindings are going to be dictated from on high, so the state mutation
  # will eventually disappear from here.  Consider pre-binding the ports prior to state machine
  # execution.
  def _on_waiting(self, process_update):
    log.debug('_on_waiting %s' % process_update)
    self._task_processes[process_update.process] = self._task_process_from_process_name(
      process_update.process)
    self._watcher.register(self._task_processes[process_update.process])
    self._planner.forget(process_update.process)

  def _on_forked(self, process_update):
    log.debug('_on_forked %s' % process_update)
    tsk_process = self._task_processes[process_update.process]
    tsk_process.set_fork_time(process_update.fork_time)
    tsk_process.set_pid(process_update.runner_pid)
    self._planner.set_running(process_update.process)

  def _on_running(self, process_update):
    log.debug('_on_running %s' % process_update)
    self._planner.set_running(process_update.process)

  @state_mutating
  def _on_finished(self, process_update):
    log.debug('_on_finished %s' % process_update)
    self._task_processes.pop(process_update.process)
    self._watcher.unregister(process_update.process)
    self._planner.set_finished(process_update.process)
    self._set_process_history_state(process_update.process, TaskRunState.SUCCESS)

  @state_mutating
  def _on_abnormal(self, process_update):
    log.debug('_on_abnormal %s' % process_update)
    self._task_processes.pop(process_update.process)
    self._watcher.unregister(process_update.process)
    process_name = process_update.process
    log.info('Process %s had an abnormal termination' % process_name)
    if self._is_process_failed(process_name):
      log.info('Process %s reached maximum failures, marking task failed.' % process_name)
      self._planner.set_finished(process_name)
      self._set_process_history_state(process_name, TaskRunState.FAILED)
    else:
      log.info('Process %s under maximum failure limit, restarting.' % process_name)
      self._planner.forget(process_update.process)
      self._dispatcher.update_runner_state(self._state,
        TaskRunnerCkpt(process_state = ProcessState(
          process = process_name, run_state = ProcessRunState.WAITING)))

  def _on_failed(self, process_update):
    log.debug('_on_failed %s' % process_update)
    self._on_abnormal(process_update)

  def _on_lost(self, process_update):
    log.debug('_on_lost %s' % process_update)
    self._on_abnormal(process_update)

  @state_mutating
  def _on_killed(self, process_update):
    log.debug('_on_killed %s' % process_update)
    self._task_processes.pop(process_update.process)
    self._set_process_history_state(process_update.process, TaskRunState.KILLED)

  def _on_port_allocation(self, name, port):
    self._port_allocator.allocate_port(name, port)

  def _register_handlers(self):
    self._dispatcher.register_universal_handler(lambda u: self._on_everything(u))
    self._dispatcher.register_state_handler(ProcessRunState.WAITING,  lambda u: self._on_waiting(u))
    self._dispatcher.register_state_handler(ProcessRunState.FORKED,   lambda u: self._on_forked(u))
    self._dispatcher.register_state_handler(ProcessRunState.RUNNING,  lambda u: self._on_running(u))
    self._dispatcher.register_state_handler(ProcessRunState.FAILED,   lambda u: self._on_failed(u))
    self._dispatcher.register_state_handler(ProcessRunState.FINISHED, lambda u: self._on_finished(u))
    self._dispatcher.register_state_handler(ProcessRunState.LOST,     lambda u: self._on_lost(u))
    self._dispatcher.register_state_handler(ProcessRunState.KILLED,   lambda u: self._on_killed(u))
    self._dispatcher.register_port_handler(lambda name, port: self._on_port_allocation(name, port))

  def _update_runner_ckpt_to_high_watermarks(self):
    process_updates = self._watcher.select()
    unapplied_process_updates = []
    for process_update in process_updates:
      if self._dispatcher.would_update(self._state, process_update):
        unapplied_process_updates.append(process_update)
      else:
        self._dispatcher.update_runner_state(self._state, process_update, recovery = True)
    self._recovery = False
    for process_update in unapplied_process_updates:
      assert self._dispatcher.update_runner_state(self._state, process_update)

  def _get_updates_from_processes(self, timeout=None):
    STAT_INTERVAL_SLEEP = Amount(1, Time.SECONDS)
    applied_updates = 0
    total_time = 0.0
    while applied_updates == 0 and (timeout is None or total_time < timeout):
      process_updates = self._watcher.select()
      for process_update in process_updates:
        if self._dispatcher.update_runner_state(self._state, process_update):
          applied_updates += 1

      # this is crazy, but necessary until we support inotify and fsevents.
      # TODO(wickman)  Factor the time module out of this so we at least stand some
      # chance of testing this.
      total_time += STAT_INTERVAL_SLEEP.as_(Time.SECONDS)
      time.sleep(STAT_INTERVAL_SLEEP.as_(Time.SECONDS))

  def _write_task(self):
    """Write a sentinel indicating that this TaskRunner is active."""
    active_task = self._pathspec.getpath('active_task_path')
    with Helper.safe_create_file(active_task, 'w') as fp:
      rw = ThriftRecordWriter(fp)
      rw.write(self._task)

  def _is_finished(self):
    """Has a finished sentinel been written indicating this TaskRunner has finished?"""
    finished_task = self._pathspec.getpath('finished_task_path')
    if not os.path.exists(finished_task):
      return None
    with open(finished_task, "r") as fp:
      rr = ThriftRecordReader(fp, ThermosTask)
      task = rr.read()
    if task is None:
      return None
    return task == self._task

  def _enforce_task_active(self):
    """Enforce that an active sentinel is around for this task."""
    active_task = self._pathspec.getpath('active_task_path')

    if not os.path.exists(active_task):
      self._write_task()
      return
    else:
      # A sentinel already exists for the active task.  Make sure it's the same.
      with open(active_task, "r") as fp:
        rr = ThriftRecordReader(fp, ThermosTask)
        task = rr.read()
      if task is None:
        log.error('Corrupt task detected! %s, overwriting...' % active_task)
        self._write_task()
      else:
        if TaskRunnerHelper.thriftify_task(self._task) != TaskRunnerHelper.thriftify_task(task):
          raise TaskRunner.InternalError(
            "Attempting to launch different tasks with same task id: new: %s, active: %s" % (
            TaskRunnerHelper.thriftify_task(self._task),
            TaskRunnerHelper.thriftify_task(task)))

  @state_mutating
  def _save_allocated_ports(self, ports):
    for name in ports:
      tap = TaskAllocatedPort(port_name = name, port = ports[name])
      runner_ckpt = TaskRunnerCkpt(allocated_port=tap)
      if self._dispatcher.update_runner_state(self._state, runner_ckpt, self._recovery):
        self._ckpt.write(runner_ckpt)

  @staticmethod
  def _current_process_run_number(task_state, process_name):
    return 0 if process_name not in task_state.processes else (
      len(task_state.processes[process_name].runs) - 1)

  @state_mutating
  def _task_process_from_process_name(self, process_name):
    """
      Construct a Process() object from a process_name, populated with its
      correct run number, leased ports and fully interpolated commandline.
      Potentially has the side-effect of getting ports allocated and
      checkpointing them.
    """
    pathspec = self._pathspec.given(
      process = process_name,
      run = self._current_process_run_number(self._state, process_name))
    process = Helper.process_from_task(self._task, process_name)
    (new_cmdline, allocated_ports) = self._port_allocator.synthesize(process.cmdline)
    log.debug('allocated ports: %s' % allocated_ports)
    self._save_allocated_ports(allocated_ports)
    # We need to deepcopy the process because this technically mutates the self._task
    # and then causes assert_active_task to fail on pre- vs. post-interpolated cmdlines.
    process = copy.deepcopy(process)
    process.cmdline = new_cmdline
    return Process(pathspec, process, Helper.process_sequence_number(self._state, process_name),
      self._sandbox, self._user, self._chroot)

  def _count_process_failures(self, process_name):
    process_failures = filter(
      lambda run: run.run_state == ProcessRunState.FAILED,
      self._state.processes[process_name].runs)
    return len(process_failures)

  def _is_process_failed(self, process_name):
    process = Helper.process_from_name(self._task, process_name)
    process_failures = self._count_process_failures(process_name)
    return process.max_failures != 0 and process_failures >= process.max_failures

  def _is_task_failed(self):
    failures = filter(
      lambda history: history.state == TaskRunState.FAILED,
      self._state.processes.values())
    return self._task.max_process_failures != 0 and (
      len(failures) >= self._task.max_process_failures)

  @staticmethod
  def safe_kill_pid(pid):
    try:
      os.kill(pid, signal.SIGKILL)
    except Exception as e:
      log.error('    (error: %s)' % e)

  def _set_lost_task_if_necessary(self, process_name):
    """
       Determine whether or not we should mark a task as LOST and do so if necessary.
    """
    assert process_name in self._task_processes
    assert len(self._state.processes[process_name].runs) > 0
    current_run = self._state.processes[process_name].runs[-1]

    def forked_but_never_came_up():
      return current_run.run_state == ProcessRunState.FORKED and (
        time.time() - current_run.fork_time > TaskRunner.LOST_TIMEOUT.as_(Time.SECONDS))

    def running_but_runner_died():
      if current_run.run_state != ProcessRunState.RUNNING:
        return False
      self._ps.collect_set(set([current_run.runner_pid]))
      if current_run.runner_pid in self._ps.pids():
        return False
      else:
        # To prevent a race condition: we must make sure that the runner pid is dead AND
        # there are no more state transitions it has written to disk.
        if self._watcher.has_data(process_name):
          return False
      return True

    if forked_but_never_came_up() or running_but_runner_died():
      log.info('Detected a LOST task: %s' % current_run)
      log.debug('  forked_but_never_came_up: %s' % forked_but_never_came_up())
      log.debug('  running_but_runner_died: %s' % running_but_runner_died())
      need_kill = current_run.pid if current_run.run_state is ProcessRunState.RUNNING else None
      self._dispatcher.update_runner_state(self._state,
        TaskRunnerCkpt(process_state = ProcessState(process = process_name,
          seq = current_run.seq + 1, run_state = ProcessRunState.LOST)))
      if need_kill:
        self._ps.collect_all()
        if need_kill not in self._ps.pids():
          log.error('PID disappeared: %s' % need_kill)
        else:
          pids = [need_kill] + list(self._ps.children_of(need_kill, all=True))
          for pid in pids:
            log.info('  Killing orphaned pid: %s' % pid)
            TaskRunner.safe_kill_pid(pid)

  def run(self):
    """
      Run the Task Runner, if necessary.  Can resume from interrupted runs.
    """
    if self._state.state != TaskState.ACTIVE:
      log.error('Cannot run task in terminal state.')
      return

    with self._control:
      # grab the running semaphore

      while True:
        self._pause_event.wait(timeout=TaskRunner.MIN_ITERATION_TIME.as_(Time.SECONDS))
        if self._pause_event.is_set():
          log.info('Detected pause event.  Breaking out of loop.')
          break

        log.debug('Schedule pass:')
        if self._run_task_state_machine():
          break

        self._enforce_task_active()

        running  = self._planner.get_running()
        finished = self._planner.get_finished()
        log.debug('running: %s' % ' '.join(running))
        log.debug('finished: %s' % ' '.join(finished))

        launched = []
        for process_name in running:
          self._set_lost_task_if_necessary(process_name)

        runnable = list(self._planner.get_runnable())
        log.debug('runnable: %s' % runnable)
        for process_name in runnable:
          if process_name not in self._task_processes:
            self._dispatcher.update_runner_state(self._state,
              TaskRunnerCkpt(process_state = ProcessState(
                process = process_name, run_state = ProcessRunState.WAITING)))

          log.info('Forking Process(%s)' % process_name)
          tp = self._task_processes[process_name]
          tp.fork()
          launched.append(tp)

        # gather and apply state transitions
        if self._planner.get_running() or len(launched) > 0:
          self._get_updates_from_processes(timeout=TaskRunner.MAX_ITERATION_TIME.as_(Time.SECONDS))

        # Call waitpid on terminating forked tasks to prevent zombies.
        while True:
          try:
            pid, status, rusage = os.wait3(os.WNOHANG)
            if pid == 0:
              break
            # TODO(wickman)  Consider using this to:
            #   1) speed up detection of LOST tasks should the runner fail for any reason
            #   2) collect aggregate resource usage of forked tasks to checkpoint
            log.debug('Detected terminated process: pid=%s, status=%s, rusage=%s' % (
              pid, status, rusage))
          except OSError as e:
            if e.errno != errno.ECHILD:
              log.warning('Unexpected error when calling waitpid: %s' % e)
            break


  @staticmethod
  def this_is_really_our_pid(pid_handle, user, start_time):
    """
      A heuristic to make sure that this is likely the pid that we own/forked.  Necessary
      because of pid-space wrapping.  We don't want to go and kill processes we don't own,
      especially if the killer is running as root.
    """
    if pid_handle.user() != user:
      return False
    estimated_start_time = time.time() - pid_handle.wall_time()
    return abs(start_time - estimated_start_time) < TaskRunner.MAX_START_TIME_DRIFT.as_(Time.SECONDS)

  def kill(self):
    """
      Kill all processes associated with this task and set task/process states as KILLED.
    """
    with self._control:
      if self._state.state != TaskState.ACTIVE:
        log.warning('Task is not in ACTIVE state, cannot issue kill.')
        return
      self._kill()

  def _set_process_kill_state(self, process_state):
    assert process_state.run_state in (ProcessRunState.RUNNING, ProcessRunState.FORKED)
    update = ProcessState(seq=process_state.seq + 1, process=process_state.process,
        stop_time=time.time(), return_code=-1, run_state=ProcessRunState.KILLED)
    runner_ckpt = TaskRunnerCkpt(process_state = update)
    log.info('Dispatching KILL state to %s' % process_state.process)
    self._dispatcher.update_runner_state(self._state, runner_ckpt)

  def _kill(self):
    log.info('Killing ThermosRunner.')
    self._set_task_state(TaskState.KILLED)
    self._ps.collect_all()

    runner_pids = []
    process_pids = []
    process_states = []

    current_user = self._state.header.user
    for process_history in self._state.processes.values():
      # collect runner_pids for runners in >=FORKED, <=RUNNING state
      last_run = process_history.runs[-1]
      if last_run.run_state in (ProcessRunState.FORKED, ProcessRunState.RUNNING):
        self._watcher.unregister(process_history.process)
        process_states.append(last_run)
        log.info('  Detected runner for %s: %s' % (process_history.process,
            last_run.runner_pid))
        if last_run.runner_pid in self._ps.pids() and TaskRunner.this_is_really_our_pid(
            self._ps.get_handle(last_run.runner_pid), current_user, last_run.fork_time):
          runner_pids.append(last_run.runner_pid)
        else:
          log.info('    (runner appears to have completed)')
      if last_run.run_state == ProcessRunState.RUNNING:
        if last_run.pid in self._ps.pids() and TaskRunner.this_is_really_our_pid(
            self._ps.get_handle(last_run.pid), current_user, last_run.start_time):
          log.info('    => Active pid: %s' % last_run.pid)
          process_pids.append(last_run.pid)
          subtree = self._ps.children_of(last_run.pid, all=True)
          if subtree:
            log.info('      => Extra children: %s' % ' '.join(map(str, subtree)))
            process_pids.extend(subtree)

    log.info('Issuing kills.')
    pid_types = { 'Runner': runner_pids, 'Child': process_pids }
    for pid_type, pid_set in pid_types.items():
      for pid in pid_set:
        log.info('  %s: %s' % (pid_type, pid))
        TaskRunner.safe_kill_pid(pid)

    for process_state in process_states:
      self._set_process_kill_state(process_state)

    log.info('Transitioning from active to finished.')
    self.cleanup()

  def cleanup(self):
    """
      Close the checkpoint and move the task checkpoint from active => finished paths.
    """
    self._ckpt.close()

    active_task_path   = self._pathspec.getpath('active_task_path')
    finished_task_path = self._pathspec.getpath('finished_task_path')
    active_exists      = os.path.exists(active_task_path)
    finished_exists    = os.path.exists(finished_task_path)

    if active_exists and not finished_exists:
      Helper.safe_create_dir(os.path.dirname(finished_task_path))
      os.rename(active_task_path, finished_task_path)
    else:
      log.error('WARNING: active_exists: %s, finished_exists: %s' % (active_exists, finished_exists))
