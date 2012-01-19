import errno
import getpass
import os
import socket
import signal
import threading
import time

from pystachio import Integer, Environment

from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_open
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from twitter.thermos.base.helper    import Helper
from twitter.thermos.base.path      import TaskPath
from twitter.thermos.base.ckpt      import TaskCkptDispatcher
from twitter.thermos.config.loader  import ThermosTaskWrapper, ThermosProcessWrapper
from twitter.thermos.config.schema  import ThermosContext
from twitter.thermos.runner.planner import Planner
from twitter.thermos.runner.ports   import PortAllocator
from twitter.thermos.runner.process import Process
from twitter.thermos.runner.muxer   import ProcessMuxer

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

class TaskRunnerHelper(object):
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
  class InvalidTaskError(Exception): pass

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

  def __init__(self, task, checkpoint_root, sandbox,
               task_id=None, portmap=None, user=None, chroot=False):
    """
      required:
        task (config.Task) = the task to run
        checkpoint_root (path) = the context in which to run the task
        sandbox (path) = the checkpoint root

      optional:
        task_id (string) = bind to this task id.  if not specified, will synthesize an id based
                           upon task.name()
        portmap (dict)   = a map (string => integer) from name to port, e.g. { 'http': 80 }
        user (string)    = the user to run the task as.  if not current user, requires setuid
                           privileges.
        chroot (boolean) = whether or not to chroot into the sandbox prior to exec.
    """
    self._ckpt = None
    typecheck = task.check()
    if not typecheck.ok():
       raise TaskRunner.InvalidTaskError('Failed to fully evaluate task: %s' %
         typecheck.message())

    self._task = task
    self._task_processes = {}
    self._watcher = ProcessMuxer()
    self._pause_event = threading.Event()
    self._control = threading.Lock()
    self._ps = ProcessProviderFactory.get()
    current_user = TaskRunnerHelper.get_actual_user()
    self._user = user or current_user
    if self._user != current_user:
      if os.geteuid() != 0:
        raise ValueError('task specifies user as %s, but %s does not have setuid permission!' % (
          self._user, current_user))

    self._chroot = chroot
    self._planner = Planner.from_task(task)
    self._port_allocator = PortAllocator(portmap)

    launch_time = time.time()
    launch_time_ms = '%06d' % int((launch_time - int(launch_time)) * 10**6)
    if not task_id:
      self._task_id = task_id = '%s-%s.%s' % (task.name(),
        time.strftime('%Y%m%d-%H%M%S', time.localtime(launch_time)),
        launch_time_ms)
    else:
      self._task_id = task_id
    self._launch_time = launch_time

    self._pathspec = TaskPath(root = checkpoint_root, task_id = self._task_id)

    # set up sandbox for running process
    self._sandbox = sandbox
    safe_mkdir(self._sandbox)

    # create runner state
    self._state      = TaskRunnerState(processes = {})
    self._dispatcher = TaskCkptDispatcher()
    self._register_handlers()

    # recover checkpointed runner state and update plan
    self._recovery = True
    self._replay_runner_ckpt()
    unapplied_updates = self._replay_process_ckpts()

    # Turn off recovery mode and start mutating stuff.
    self._recovery = False
    self._initialize_ckpt_header()
    self._replay(unapplied_updates)

  def state(self):
    return self._state

  def __del__(self):
    if hasattr(self, '_ckpt') and self._ckpt is not None:
      self._ckpt.close()

  def _initialize_mutable_ckpt(self):
    """
      Bind to the checkpoint associated with this task, position to the end of the log if
      it exists, or create it if it doesn't.
    """
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    fp = safe_open(ckpt_file, "a")
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    self._ckpt = ckpt

  def state_mutating(fn):
    """
      Denote that a function is a potentially state mutating function.  This automatically
      constructs the checkpoint stream on a state change should we be out of recovery mode.
    """
    def _wrapper(self, *args, **kwargs):
      if self._ckpt is None and self._recovery is False:
        self._initialize_mutable_ckpt()
      return fn(self, *args, **kwargs)
    return _wrapper

  @state_mutating
  def _ckpt_write(self, record):
    """
      Write to the checkpoint if we're not in recovery mode.
    """
    if not self._recovery:
      self._ckpt.write(record)

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
    self._pause_event.clear()

  def _replay_runner_ckpt(self):
    """
      Replay the checkpoint associated with this task.
    """
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    if os.path.exists(ckpt_file):
      fp = open(ckpt_file, "r")
      ckpt_recover = ThriftRecordReader(fp, TaskRunnerCkpt)
      for record in ckpt_recover:
        self._dispatcher.update_runner_state(self._state, record, recovery=True)
      ckpt_recover.close()

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
        self._dispatcher.update_runner_state(self._state, process_update, recovery=True)
    return unapplied_process_updates

  @state_mutating
  def _replay(self, checkpoints):
    """
      Replay a sequence of TaskRunnerCkpts.
    """
    for checkpoint in checkpoints:
      self._dispatcher.update_runner_state(self._state, checkpoint)

  @state_mutating
  def _initialize_ckpt_header(self):
    """
      Initializes the TaskRunnerHeader for this checkpoint stream if it has not already
      been constructed.
    """
    if self._state.header is None:
      header = TaskRunnerHeader(
        task_id = self._task_id,
        launch_time = int(self._launch_time),
        sandbox = self._sandbox,
        hostname = socket.gethostname(),
        user = self._user)
      runner_ckpt = TaskRunnerCkpt(runner_header=header)
      if self._dispatcher.update_runner_state(self._state, runner_ckpt):
        self._ckpt_write(runner_ckpt)

  @state_mutating
  def _run_task_state_machine(self):
    """
      Run the task state machine, returning True if a terminal state has been reached.
    """
    if self._state.state is None:
      self._set_task_state(TaskState.ACTIVE)
      return False

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
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery=self._recovery):
      self._ckpt_write(runner_ckpt)

  @state_mutating
  def _set_process_history_state(self, process, state):
    update = TaskRunStateUpdate(process = process, state = state)
    runner_ckpt = TaskRunnerCkpt(history_state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery=self._recovery):
      self._ckpt_write(runner_ckpt)

  @state_mutating
  def _save_allocated_port(self, port_name, port_number):
    tap = TaskAllocatedPort(port_name = port_name, port = port_number)
    runner_ckpt = TaskRunnerCkpt(allocated_port=tap)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, self._recovery):
      self._ckpt.write(runner_ckpt)

  # process transitions for the state machine
  @state_mutating
  def _on_everything(self, process_update):
    self._ckpt_write(TaskRunnerCkpt(process_state = process_update))

  @state_mutating
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
    self._port_allocator.allocate(name, port)

  def _register_handlers(self):
    self._dispatcher.register_universal_handler(self._on_everything)
    self._dispatcher.register_port_handler(self._on_port_allocation)

    self._dispatcher.register_state_handler(ProcessRunState.WAITING,  self._on_waiting)
    self._dispatcher.register_state_handler(ProcessRunState.FORKED,   self._on_forked)
    self._dispatcher.register_state_handler(ProcessRunState.RUNNING,  self._on_running)
    self._dispatcher.register_state_handler(ProcessRunState.FAILED,   self._on_failed)
    self._dispatcher.register_state_handler(ProcessRunState.FINISHED, self._on_finished)
    self._dispatcher.register_state_handler(ProcessRunState.LOST,     self._on_lost)
    self._dispatcher.register_state_handler(ProcessRunState.KILLED,   self._on_killed)

  def _get_updates_from_processes(self, timeout=None):
    STAT_INTERVAL_SLEEP = Amount(1, Time.SECONDS)
    applied_updates = 0
    total_time = 0.0
    while applied_updates == 0 and (timeout is None or total_time < timeout):
      process_updates = self._watcher.select()
      for process_update in process_updates:
        if self._dispatcher.update_runner_state(self._state, process_update):
          applied_updates += 1
      # TODO(wickman)  Factor the time module out of this so we at least stand some
      # chance of testing this.
      total_time += STAT_INTERVAL_SLEEP.as_(Time.SECONDS)
      time.sleep(STAT_INTERVAL_SLEEP.as_(Time.SECONDS))

  def _write_task(self):
    """Write a sentinel indicating that this TaskRunner is active."""
    # PYSTACHIO(wickman)
    active_task = self._pathspec.getpath('active_task_path')
    ThermosTaskWrapper(self._task).to_file(active_task)

  @staticmethod
  def tasks_equal(filename, reified_task):
    if not os.path.exists(filename):
      return False
    task = ThermosTaskWrapper.from_file(filename)
    if not task:
      return False
    return task.task == reified_task

  def _enforce_task_active(self):
    """Enforce that an active sentinel is around for this task."""
    active_task = self._pathspec.getpath('active_task_path')

    if not os.path.exists(active_task):
      self._write_task()
      return
    else:
      # A sentinel already exists for the active task.  Make sure it's the same.
      task = ThermosTaskWrapper.from_file(active_task)
      if task is None:
        log.error('Corrupt task detected! %s, overwriting...' % active_task)
        self._write_task()
      else:
        # PYSTACHIO(wickman)
        if task.task != self._task:
          raise TaskRunner.InternalError(
            "Attempting to launch different tasks with same task id: new: %s, active: %s" % (
              task.task, self._task))

  @staticmethod
  def _current_process_run_number(task_state, process_name):
    return 0 if process_name not in task_state.processes else (
      len(task_state.processes[process_name].runs) - 1)

  def _task_process_from_process_name(self, process_name):
    """
      Construct a Process() object from a process_name, populated with its
      correct run number and fully interpolated commandline.
    """
    def process_sequence_number(task_state):
      if process_name not in task_state.processes:
        return 0
      else:
        return task_state.processes[process_name].runs[-1].seq
    pathspec = self._pathspec.given(
      process = process_name,
      run = self._current_process_run_number(self._state, process_name))
    process = Helper.process_from_name(self._task, process_name)
    ports = ThermosProcessWrapper(process).ports()
    portmap = {}
    for port_name in ports:
      allocated, port_number = self._port_allocator.allocate(port_name)
      portmap[port_name] = port_number
      if allocated:
        self._save_allocated_port(port_name, port_number)
    context = ThermosContext(task_id = self._task_id,
                             user = self._user,
                             ports = portmap)
    process, uninterp = (process % Environment(thermos = context)).interpolate()
    assert len(uninterp) == 0, 'Failed to interpolate process, missing:\n%s' % (
      '\n'.join(str(ref) for ref in uninterp))
    return Process(pathspec, process, process_sequence_number(self._state),
      self._sandbox, self._user, self._chroot)

  def _count_process_failures(self, process_name):
    process_failures = filter(
      lambda run: run.run_state == ProcessRunState.FAILED,
      self._state.processes[process_name].runs)
    return len(process_failures)

  def _is_process_failed(self, process_name):
    process = Helper.process_from_name(self._task, process_name)
    # TODO(wickman) Should pystachio coerce for __gt__/__lt__ ?
    process_failures = Integer(self._count_process_failures(process_name))
    log.debug('process_name: %s, process = %s, process_failures = %s' % (
      process_name, process, process_failures))
    return process.max_failures() != Integer(0) and process_failures >= process.max_failures()

  def _is_task_failed(self):
    # PYSTACHIO(wickman)
    failures = filter(
      lambda history: history.state == TaskRunState.FAILED,
      self._state.processes.values())
    return self._task.max_failures() != Integer(0) and (
      Integer(len(failures)) >= self._task.max_failures())

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
        log.debug('runnable: %s' % ' '.join(runnable))
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
        # ProcessFactory.reap_children()
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
      self._run_task_state_machine()
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
      safe_mkdir(os.path.dirname(finished_task_path))
      os.rename(active_task_path, finished_task_path)
    else:
      log.error('WARNING: active_exists: %s, finished_exists: %s' % (active_exists, finished_exists))

  del state_mutating
