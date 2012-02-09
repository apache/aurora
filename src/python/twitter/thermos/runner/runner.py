import errno
import getpass
import os
import socket
import signal
import threading
import time
from contextlib import contextmanager

from pystachio import Integer, Environment

from twitter.common import log
from twitter.common.dirutil import (
  safe_mkdir,
  safe_open,
  lock_file,
  unlock_file)
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from twitter.thermos.base.path import TaskPath
from twitter.thermos.base.ckpt import (
  TaskCkptDispatcher,
  UniversalStateHandler,
  ProcessStateHandler,
  TaskStateHandler)
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
  RunnerCkpt,
  RunnerHeader,
  RunnerState,
  TaskRunState,
  TaskRunStateUpdate,
  TaskState,
  TaskStatus,
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
      return thrift_deserialize(RunnerState(), fp.read())

  @staticmethod
  def get_actual_user():
    import pwd
    try:
      pwd_entry = pwd.getpwuid(os.getuid())
    except KeyError:
      return getpass.getuser()
    return pwd_entry[0]

  @staticmethod
  def process_from_name(task, process_name):
    if task.has_processes():
      for process in task.processes():
        if process.name().get() == process_name:
          return process
    return None


# TODO(wickman) Currently this is messy because of all the private access into ._runner.
# Clean this up by giving the TaskRunnerProcessHandler the components it should own, and
# create a legitimate API contract into the Runner.
class TaskRunnerProcessHandler(ProcessStateHandler):
  """
    Accesses these parts of the runner:

               | _task_processes [array]
      Related: | _task_processes_from_process_name [mapping function]
               | Process [set_fork_time, set_pid]
               | _watcher [ProcessMuxer.register, unregister]
               | Consider refactoring into something like:
               | ProcessManager(self._task, self._pathspec)
               |   .has_process(name) => yes/no
               |   .activate(name, sequence_number=0)
               |   .deactivate(name)
               |   .get(name) => unfortunately requires checkpoint record generation
               |      due to port acquisition.
               |
               | ProcessFactory => Process(*1, *2, *3, pathspec, sandbox, user, chroot, fork)
               |   *1 => name
               |   *2 => cmdline
               |   *3 => sequence number

      _planner [Planner.forget, Planner.set_finished]
      _set_process_history_state [state transition function => potentially ckpt mutable]
  """

  def __init__(self, runner):
    self._runner = runner

  def on_waiting(self, process_update):
    log.debug('Process on_waiting %s' % process_update)
    self._runner._task_processes[process_update.process] = self._runner._task_process_from_process_name(
      process_update.process, sequence_number=process_update.seq)
    self._runner._watcher.register(self._runner._task_processes[process_update.process])
    self._runner._planner.forget(process_update.process)

  def on_forked(self, process_update):
    log.debug('Process on_forked %s' % process_update)
    task_process = self._runner._task_processes[process_update.process]
    task_process.rebind(process_update.coordinator_pid, process_update.fork_time)
    self._runner._planner.set_running(process_update.process)

  def on_running(self, process_update):
    log.debug('Process on_running %s' % process_update)
    self._runner._planner.set_running(process_update.process)

  def on_finished(self, process_update):
    log.debug('Process on_finished %s' % process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    self._runner._planner.set_finished(process_update.process)
    self._runner._set_process_history_state(process_update.process, TaskRunState.SUCCESS)

  def _on_abnormal(self, process_update):
    log.debug('  => _on_abnormal %s' % process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    process_name = process_update.process
    log.info('Process %s had an abnormal termination' % process_name)
    if self._runner._is_process_failed(process_name):
      log.info('Process %s reached maximum failures, marking process run failed.' % process_name)
      self._runner._planner.set_broken(process_update.process)
      self._runner._set_process_history_state(process_name, TaskRunState.FAILED)
    else:
      log.info('Process %s under maximum failure limit, restarting.' % process_name)
      self._runner._planner.forget(process_update.process)
      self._runner._set_process_state(process_name, ProcessRunState.WAITING)

  def on_failed(self, process_update):
    log.debug('Process on_failed %s' % process_update)
    self._on_abnormal(process_update)

  def on_lost(self, process_update):
    log.debug('Process on_lost %s' % process_update)
    self._on_abnormal(process_update)

  def on_killed(self, process_update):
    log.debug('Process on_killed %s' % process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    self._runner._set_process_history_state(process_update.process, TaskRunState.KILLED)


class TaskRunnerTaskHandler(TaskStateHandler):
  """
    Accesses these parts of the runner:
      _recovery [boolean, whether or not to side-effect]
      _pathspec [path creation]
      _task [ThermosTask]
  """

  def __init__(self, runner):
    self._runner = runner
    self._pathspec = self._runner._pathspec

  def on_active(self, task_update):
    log.debug('Task on_active' % task_update)
    if self._runner._recovery:
      return
    active_task = self._pathspec.given(state='active').getpath('task_path')
    finished_task = self._pathspec.given(state='finished').getpath('task_path')
    is_active, is_finished = map(os.path.exists, [active_task, finished_task])
    assert not is_finished
    if is_active:
      assert TaskRunner.tasks_equal(active_task, self._runner._task), (
        "Attempting to run a different task by the same name.")
    else:
      ThermosTaskWrapper(self._runner._task).to_file(active_task)

  def on_terminal(self):
    active_task = self._pathspec.given(state='active').getpath('task_path')
    finished_task = self._pathspec.given(state='finished').getpath('task_path')
    is_active, is_finished = map(os.path.exists, [active_task, finished_task])
    assert is_active and not is_finished
    safe_mkdir(os.path.dirname(finished_task))
    os.rename(active_task, finished_task)

  def on_success(self, task_update):
    log.debug('Task on_success' % task_update)
    if not self._runner._recovery:
      self.on_terminal()

  def killkillkill(self):
    if not self._runner._recovery:
      self.on_terminal()
    # TODO(wickman) This happens even on checkpoint replay.  Perhaps this
    # intention should be parameterized since certainly not everyone doing a
    # drive-by checkpoint inspection will want to be trying to kill
    # processes that perhaps they do not even own.
    self._runner._kill()

  def on_failed(self, task_update):
    log.debug('Task on_failed' % task_update)
    self.killkillkill()

  def on_killed(self, task_update):
    log.debug('Task on_killed' % task_update)
    self.killkillkill()


class TaskRunnerUniversalHandler(UniversalStateHandler):
  """
    Accesses these parts of the runner:
      _ckpt_write
  """

  def __init__(self, runner):
    self._runner = runner

  def on_process_transition(self, state, process_update):
    log.debug('_on_process_transition: %s' % process_update)
    self._runner._ckpt_write(RunnerCkpt(process_state=process_update))

  def on_process_history_transition(self, state, process_history_update):
    log.debug('_on_process_history_transition: %s' % process_history_update)
    self._runner._ckpt_write(RunnerCkpt(history_state_update=process_history_update))

  def on_task_transition(self, state, task_update):
    log.debug('_on_task_transition: %s' % task_update)
    self._runner._ckpt_write(RunnerCkpt(status_update=task_update))

  def on_port_allocation(self, name, port):
    # self._runner._port_allocator.allocate(name, port) ? necessary
    log.debug('_on_port_allocation: %s=>%s' % (name, port))
    self._runner._ckpt_write(RunnerCkpt(allocated_port=TaskAllocatedPort(
      port_name=name, port=port)))

  def on_initialization(self, header):
    log.debug('_on_initialization: %s' % header)
    self._runner._ckpt_write(RunnerCkpt(runner_header=header))


class TaskRunner(object):
  """
    Run a ThermosTask.
  """

  class Error(Exception): pass
  class InternalError(Error): pass
  class InvalidTaskError(Error): pass
  class PermissionError(Error): pass
  class StateError(Error): pass

  # Maximum drift between when the system says a task was forked and when we checkpointed
  # its fork_time (used as a heuristic to determine a forked task is really ours instead of
  # a task with coincidentally the same PID but just wrapped around.)
  MAX_START_TIME_DRIFT = Amount(10, Time.SECONDS)

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

    # create runner state
    self._state      = RunnerState(processes = {})
    self._dispatcher = TaskCkptDispatcher()
    self._register_handlers()

    # recover checkpointed runner state and update plan
    self._recovery = True
    self._replay_runner_ckpt()

  def state(self):
    return self._state

  def task_state(self):
    return self._state.statuses[-1].state if self._state.statuses else TaskState.ACTIVE

  def kill_current_runner(self):
    assert self._state.statuses
    pid = self._state.statuses[-1].runner_pid
    assert pid != os.getpid(), 'Unwilling to commit seppuku.'
    try:
      os.kill(pid, signal.SIGKILL)
      return True
    except OSError as e:
      if e.errno == errno.EPERM:
        # Permission denied
        return False
      elif e.errno == errno.ESRCH:
        # pid no longer exists
        return True
      raise

  @contextmanager
  def control(self, force=False):
    """
      Bind to the checkpoint associated with this task, position to the end of the log if
      it exists, or create it if it doesn't.  Fails if we cannot get "leadership" i.e. a
      file lock on the checkpoint stream.
    """
    if self.task_state() != TaskState.ACTIVE:
      raise TaskRunner.StateError('Cannot take control of a task in terminal state.')
    safe_mkdir(self._sandbox)
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    safe_mkdir(os.path.dirname(ckpt_file))
    fp = lock_file(ckpt_file, "a")
    if fp in (None, False):
      if force:
        log.info('Found existing runner, forcing leadership forfeit.')
        if self.kill_current_runner():
          log.info('Successfully killed leader.')
          fp = lock_file(ckpt_file, "a")
      else:
        log.error('Found existing runner, cannot take control.')
    if fp in (None, False):
      raise TaskRunner.PermissionError('Could not open locked checkpoint: %s' % ckpt_file)
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    self._ckpt = ckpt
    log.debug('Flipping recovery mode off.')
    self._recovery = False
    self._set_task_state(TaskState.ACTIVE)
    self._resume_task()
    yield
    self._ckpt.close()

  def _resume_task(self):
    assert self._ckpt is not None
    unapplied_updates = self._replay_process_ckpts()
    assert self.task_state() == TaskState.ACTIVE, 'Should not resume inactive task.'
    self._initialize_ckpt_header()
    self._replay(unapplied_updates)

  def _ckpt_write(self, record):
    """
      Write to the checkpoint if we're not in recovery mode.
    """
    if not self._recovery:
      self._ckpt.write(record)

  def _replay_runner_ckpt(self):
    """
      Replay the checkpoint associated with this task.
    """
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    if os.path.exists(ckpt_file):
      fp = open(ckpt_file, "r")
      ckpt_recover = ThriftRecordReader(fp, RunnerCkpt)
      for record in ckpt_recover:
        log.debug('Replaying runner checkpoint record: %s' % record)
        self._dispatcher.dispatch(self._state, record, recovery=True)
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
        self._dispatcher.dispatch(self._state, process_update, recovery=True)
    return unapplied_process_updates

  def _replay(self, checkpoints):
    """
      Replay a sequence of RunnerCkpts.
    """
    for checkpoint in checkpoints:
      self._dispatcher.dispatch(self._state, checkpoint)

  def _initialize_ckpt_header(self):
    """
      Initializes the RunnerHeader for this checkpoint stream if it has not already
      been constructed.
    """
    if self._state.header is None:
      header = RunnerHeader(
        task_id = self._task_id,
        launch_time_ms = int(self._launch_time*1000),
        sandbox = self._sandbox,
        hostname = socket.gethostname(),
        user = self._user)
      runner_ckpt = RunnerCkpt(runner_header=header)
      self._dispatcher.dispatch(self._state, runner_ckpt)

  def _run_task_state_machine(self):
    """
      Run the task state machine, returning True if a terminal state has been reached.
    """
    # State machine should only be run after checkpoint acquisition, which prepends
    # active status.
    assert self._state.statuses not in (None, [])
    if self._is_task_failed():
      self._set_task_state(TaskState.FAILED)
      return True
    elif self._planner.is_complete():
      self._set_task_state(TaskState.SUCCESS)
      return True
    return False

  def _set_task_state(self, state):
    update = TaskStatus(state = state, timestamp_ms = int(time.time() * 1000),
                        runner_pid = os.getpid())
    runner_ckpt = RunnerCkpt(status_update = update)
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _set_process_state(self, process_name, process_state, **kw):
    runner_ckpt = RunnerCkpt(process_state = ProcessState(
      process = process_name, run_state = process_state, **kw))
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _set_process_history_state(self, process, state):
    update = TaskRunStateUpdate(process = process, state = state)
    runner_ckpt = RunnerCkpt(history_state_update = update)
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _set_allocated_port(self, port_name, port_number):
    tap = TaskAllocatedPort(port_name = port_name, port = port_number)
    runner_ckpt = RunnerCkpt(allocated_port=tap)
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _register_handlers(self):
    self._dispatcher.register_handler(TaskRunnerUniversalHandler(self))
    self._dispatcher.register_handler(TaskRunnerProcessHandler(self))
    self._dispatcher.register_handler(TaskRunnerTaskHandler(self))

  def _get_updates_from_processes(self, timeout=None):
    STAT_INTERVAL_SLEEP = Amount(1, Time.SECONDS)
    applied_updates = 0
    total_time = 0.0
    while applied_updates == 0 and (timeout is None or total_time < timeout):
      process_updates = self._watcher.select()
      for process_update in process_updates:
        if self._dispatcher.dispatch(self._state, process_update):
          applied_updates += 1
      # TODO(wickman)  Factor the time module out of this so we at least stand some
      # chance of testing this.
      total_time += STAT_INTERVAL_SLEEP.as_(Time.SECONDS)
      time.sleep(STAT_INTERVAL_SLEEP.as_(Time.SECONDS))

  @staticmethod
  def tasks_equal(filename, reified_task):
    if not os.path.exists(filename):
      return False
    task = ThermosTaskWrapper.from_file(filename)
    if not task:
      return False
    return task.task == reified_task

  def _task_process_from_process_name(self, process_name, sequence_number=0):
    """
      Construct a Process() object from a process_name, populated with its
      correct run number and fully interpolated commandline.
    """
    run_number = len(self.state().processes[process_name].runs)
    pathspec = self._pathspec.given(process = process_name, run = run_number)
    process = TaskRunnerHelper.process_from_name(self._task, process_name)
    ports = ThermosProcessWrapper(process).ports()
    portmap = {}
    for port_name in ports:
      allocated, port_number = self._port_allocator.allocate(port_name)
      portmap[port_name] = port_number
      if allocated:
        self._set_allocated_port(port_name, port_number)
    context = ThermosContext(task_id = self._task_id,
                             user = self._user,
                             ports = portmap)
    process, uninterp = (process % Environment(thermos = context)).interpolate()
    assert len(uninterp) == 0, 'Failed to interpolate process, missing:\n%s' % (
      '\n'.join(str(ref) for ref in uninterp))
    # close the ckpt on the Process, also consider closing down logging or recreating the
    # logging lock.
    def close_ckpt_and_fork():
      pid = os.fork()
      if pid == 0 and self._ckpt is not None:
        self._ckpt.close()
      return pid
    return Process(
      process.name().get(),
      process.cmdline().get(),
      sequence_number,
      pathspec,
      self._sandbox, self._user, self._chroot, fork=close_ckpt_and_fork)

  def _count_process_failures(self, process_name):
    process_failures = filter(
      lambda run: run.run_state == ProcessRunState.FAILED,
      self._state.processes[process_name].runs)
    return len(process_failures)

  def _is_process_failed(self, process_name):
    process = TaskRunnerHelper.process_from_name(self._task, process_name)
    # TODO(wickman) Should pystachio coerce for __gt__/__lt__ ?
    process_failures = Integer(self._count_process_failures(process_name))
    log.debug('process_name: %s, process = %s, process_failures = %s' % (
      process_name, process, process_failures))
    return process.max_failures() != Integer(0) and process_failures >= process.max_failures()

  def _is_task_failed(self):
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
      self._ps.collect_set(set([current_run.coordinator_pid]))
      if current_run.coordinator_pid in self._ps.pids():
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
      self._set_process_state(process_name, ProcessRunState.LOST, seq = current_run.seq + 1)
      if need_kill:
        self._ps.collect_all()
        if need_kill not in self._ps.pids():
          log.error('PID disappeared: %s' % need_kill)
        else:
          pids = [need_kill] + list(self._ps.children_of(need_kill, all=True))
          for pid in pids:
            log.info('  Killing orphaned pid: %s' % pid)
            TaskRunner.safe_kill_pid(pid)

  def run(self, force=False):
    """
      Run the Task Runner, if necessary.  Can resume from interrupted runs.
    """
    with self.control(force):
      while True:
        log.debug('Schedule pass:')
        if self._run_task_state_machine():
          break

        running  = self._planner.get_running()
        finished = self._planner.get_finished()
        log.debug('running: %s' % ' '.join(running))
        log.debug('finished: %s' % ' '.join(finished))

        launched = []
        for process_name in running:
          self._set_lost_task_if_necessary(process_name)

        runnable = list(self._planner.get_runnable())
        log.debug('runnable: %s' % ' '.join(runnable))

        if len(running) == 0 and len(runnable) == 0 and not self._planner.is_complete():
          log.error('Terminating Task because nothing is runnable.')
          self._set_task_state(TaskState.FAILED)
          break

        for process_name in runnable:
          if process_name not in self._task_processes:
            self._set_process_state(process_name, ProcessRunState.WAITING)

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
            #   1) speed up detection of LOST tasks should the coordinator fail for any reason
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

  def kill(self, force=False):
    """
      Kill all processes associated with this task and set task/process states as KILLED.
    """
    with self.control(force):
      self._run_task_state_machine()
      if self.task_state() != TaskState.ACTIVE:
        log.warning('Task is not in ACTIVE state, cannot issue kill.')
        return
      self._set_task_state(TaskState.KILLED)

  def _set_process_kill_state(self, process_state):
    assert process_state.run_state in (ProcessRunState.RUNNING, ProcessRunState.FORKED)
    update = ProcessState(seq=process_state.seq + 1, process=process_state.process,
        stop_time=time.time(), return_code=-1, run_state=ProcessRunState.KILLED)
    runner_ckpt = RunnerCkpt(process_state = update)
    log.info('Dispatching KILL state to %s' % process_state.process)
    self._dispatcher.dispatch(self._state, runner_ckpt)

  def _kill(self):
    log.info('Killing ThermosRunner.')
    self._ps.collect_all()

    coordinator_pids = []
    process_pids = []
    process_states = []

    current_user = self._state.header.user
    for process_history in self._state.processes.values():
      # collect coordinator_pids for runners in >=FORKED, <=RUNNING state
      last_run = process_history.runs[-1]
      if last_run.run_state in (ProcessRunState.FORKED, ProcessRunState.RUNNING):
        self._watcher.unregister(process_history.process)
        process_states.append(last_run)
        log.info('  Detected runner for %s: %s' % (process_history.process,
            last_run.coordinator_pid))
        if last_run.coordinator_pid in self._ps.pids() and TaskRunner.this_is_really_our_pid(
            self._ps.get_handle(last_run.coordinator_pid), current_user, last_run.fork_time):
          coordinator_pids.append(last_run.coordinator_pid)
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
    pid_types = { 'Coordinator': coordinator_pids, 'Child': process_pids }
    for pid_type, pid_set in pid_types.items():
      for pid in pid_set:
        log.info('  %s: %s' % (pid_type, pid))
        TaskRunner.safe_kill_pid(pid)

    for process_state in process_states:
      self._set_process_kill_state(process_state)

    log.info('Kill complete.')
