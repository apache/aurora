from contextlib import contextmanager
import errno
import getpass
import os
import socket
import signal
import time
import traceback

from pystachio import Integer, Environment

from twitter.common import log
from twitter.common.dirutil import safe_mkdir, lock_file
from twitter.common.process import ProcessProviderFactory
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from twitter.thermos.base.path import TaskPath
from twitter.thermos.base.ckpt import (
  CheckpointDispatcher,
  UniversalStateHandler,
  ProcessStateHandler,
  TaskStateHandler)
from twitter.thermos.config.loader import (
  ThermosConfigLoader,
  ThermosTaskWrapper,
  ThermosProcessWrapper)
from twitter.thermos.config.schema import ThermosContext
from twitter.thermos.runner.planner import Planner
from twitter.thermos.runner.process import Process
from twitter.thermos.runner.muxer import ProcessMuxer

from gen.twitter.thermos.ttypes import (
  ProcessState,
  ProcessStatus,
  RunnerCkpt,
  RunnerHeader,
  RunnerState,
  TaskState,
  TaskStatus,
)

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False


def safe_kill_pid(pid):
  try:
    os.kill(pid, signal.SIGKILL)
  except Exception as e:
    log.error('    (error: %s)' % e)


class TaskRunnerHelper(object):
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
               |   .get(name) => ProcessHandle ???
               |   .reap()
               |
               | ProcessFactory => Process(*1, *2, *3, pathspec, sandbox, user, chroot, fork)
               |   *1 => name
               |   *2 => cmdline
               |   *3 => sequence number

      _planner [Planner.forget, Planner.set_finished]
        ==> Could make this completely stateless and instead make its decisions based upon
            state.processes
  """

  def __init__(self, runner):
    self._runner = runner

  def on_waiting(self, process_update):
    log.debug('Process on_waiting %s' % process_update)
    self._runner._task_processes[process_update.process] = (
      self._runner._task_process_from_process_name(
        process_update.process, process_update.seq + 1))
    self._runner._watcher.register(process_update.process, process_update.seq - 1)
    self._runner._planner.forget(process_update.process)

  def on_forked(self, process_update):
    log.debug('Process on_forked %s' % process_update)
    task_process = self._runner._task_processes[process_update.process]
    task_process.rebind(process_update.coordinator_pid, process_update.fork_time)
    self._runner._planner.set_running(process_update.process)

  def on_running(self, process_update):
    log.debug('Process on_running %s' % process_update)
    self._runner._planner.set_running(process_update.process)

  def on_success(self, process_update):
    log.debug('Process on_success %s' % process_update)
    log.info('Process(%s) finished successfully [rc=%s]' % (
      process_update.process, process_update.return_code))
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    self._runner._planner.set_finished(process_update.process)

  def _on_abnormal(self, process_update):
    log.debug('  => _on_abnormal %s' % process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)
    process_name = process_update.process
    log.info('Process %s had an abnormal termination' % process_name)
    if self._runner._is_process_failed(process_name):
      log.info('Process %s reached maximum failures, marking process run failed.' % process_name)
      self._runner._planner.set_broken(process_update.process)
    else:
      log.info('Process %s under maximum failure limit, restarting.' % process_name)
      self._runner._planner.forget(process_update.process)
      self._runner._set_process_status(process_name, ProcessState.WAITING, process_update.seq + 1)

  def on_failed(self, process_update):
    log.debug('Process on_failed %s' % process_update)
    log.info('Process(%s) failed [rc=%s]' % (
      process_update.process, process_update.return_code))
    self._on_abnormal(process_update)

  def on_lost(self, process_update):
    log.debug('Process on_lost %s' % process_update)
    self._on_abnormal(process_update)

  def on_killed(self, process_update):
    log.debug('Process on_killed %s' % process_update)
    self._runner._task_processes.pop(process_update.process)
    self._runner._watcher.unregister(process_update.process)


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

    def tasks_equal(filename, reified_task):
      if not os.path.exists(filename):
        return False
      task = ThermosTaskWrapper.from_file(filename)
      if not task:
        return False
      return task.task == reified_task

    if is_active:
      assert tasks_equal(active_task, self._runner._task), (
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
    os.utime(finished_task, None)

  def on_success(self, task_update):
    log.debug('Task on_success' % task_update)
    log.info('Task succeeded.')
    if not self._runner._recovery:
      self.on_terminal()

  def killkillkill(self):
    # TODO(wickman) This happens even on checkpoint replay.  Perhaps this
    # intention should be parameterized since certainly not everyone doing a
    # drive-by checkpoint inspection will want to be trying to kill
    # processes that perhaps they do not even own.
    self._runner._kill()
    if not self._runner._recovery:
      self.on_terminal()

  def on_lost(self, task_update):
    log.debug('Task on_lost' % task_update)
    self.killkillkill()

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
    self._runner._ckpt_write(RunnerCkpt(process_status=process_update))

  def on_task_transition(self, state, task_update):
    log.debug('_on_task_transition: %s' % task_update)
    self._runner._ckpt_write(RunnerCkpt(task_status=task_update))

  def on_initialization(self, header):
    log.debug('_on_initialization: %s' % header)
    TaskRunner.assert_valid_task(self._runner.task, header.ports)
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

  @staticmethod
  def assert_valid_task(task, portmap):
    for process in task.processes():
      name = process.name().get()
      ThermosProcessWrapper.assert_valid_process_name(name)
    for port in ThermosTaskWrapper(task).ports():
      if port not in portmap:
        raise TaskRunner.InvalidTaskError('Task requires unbound port %s!' % port)
    typecheck = task.check()
    if not typecheck.ok():
      raise TaskRunner.InvalidTaskError('Failed to fully evaluate task: %s' %
        typecheck.message())

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
    try:
      checkpoint = CheckpointDispatcher.from_file(task_checkpoint)
      return cls(task.tasks()[0].task(), checkpoint_root, checkpoint.header.sandbox, task_id,
          portmap=checkpoint.header.ports)
    except Exception as e:
      log.error('Failed to reconstitute checkpoint in TaskRunner.get: %s' % e, exc_info=True)
      return None

  def __init__(self, task, checkpoint_root, sandbox,
               task_id=None, portmap=None, user=None, chroot=False, clock=time):
    """
      required:
        task (config.Task) = the task to run
        checkpoint_root (path) = the checkpoint root
        sandbox (path) = the sandbox in which the path will be run
                         [if None, cwd will be assumed, but garbage collection will be
                          disalbed for this task.]

      optional:
        task_id (string) = bind to this task id.  if not specified, will synthesize an id based
                           upon task.name()
        portmap (dict)   = a map (string => integer) from name to port, e.g. { 'http': 80 }
        user (string)    = the user to run the task as.  if not current user, requires setuid
                           privileges.
        chroot (boolean) = whether or not to chroot into the sandbox prior to exec.
        clock (time interface) = the clock to use throughout
    """
    self._portmap = portmap or {}
    TaskRunner.assert_valid_task(task, self._portmap)

    self._ckpt = None
    self._task = task
    self._task_processes = {}
    self._ps = ProcessProviderFactory.get()
    current_user = TaskRunnerHelper.get_actual_user()
    self._user = user or current_user
    if self._user != current_user:
      if os.geteuid() != 0:
        raise ValueError('task specifies user as %s, but %s does not have setuid permission!' % (
          self._user, current_user))

    self._chroot = chroot
    self._planner = Planner.from_task(task)
    self._clock = clock

    launch_time = self._clock.time()
    launch_time_ms = '%06d' % int((launch_time - int(launch_time)) * 10**6)
    if not task_id:
      self._task_id = task_id = '%s-%s.%s' % (task.name(),
        time.strftime('%Y%m%d-%H%M%S', time.localtime(launch_time)),
        launch_time_ms)
    else:
      self._task_id = task_id
    self._launch_time = launch_time
    self._pathspec = TaskPath(root = checkpoint_root, task_id = self._task_id)
    self._watcher = ProcessMuxer(self._pathspec)

    # set up sandbox for running process
    self._sandbox = sandbox

    # create runner state
    self._state      = RunnerState(processes = {})
    self._dispatcher = CheckpointDispatcher()
    self._dispatcher.register_handler(TaskRunnerUniversalHandler(self))
    self._dispatcher.register_handler(TaskRunnerProcessHandler(self))
    self._dispatcher.register_handler(TaskRunnerTaskHandler(self))

    # recover checkpointed runner state and update plan
    self._recovery = True
    self._replay_runner_ckpt()

  @property
  def task(self):
    return self._task

  @property
  def state(self):
    return self._state

  @property
  def processes(self):
    return self._task_processes

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
    if self._sandbox:
      safe_mkdir(self._sandbox)
    ckpt_file = self._pathspec.getpath('runner_checkpoint')
    safe_mkdir(os.path.dirname(ckpt_file))
    fp = lock_file(ckpt_file, "a+")
    if fp in (None, False):
      if force:
        log.info('Found existing runner, forcing leadership forfeit.')
        if self.kill_current_runner():
          log.info('Successfully killed leader.')
          # TODO(wickman)  Blocking may not be the best idea here.  Perhaps block up to
          # a maximum timeout.  But blocking is necessary because os.kill does not immediately
          # release the lock if we're in force mode.
          fp = lock_file(ckpt_file, "a+", blocking=True)
      else:
        log.error('Found existing runner, cannot take control.')
    if fp in (None, False):
      raise TaskRunner.PermissionError('Could not open locked checkpoint: %s, lock_file = %s' %
        (ckpt_file, fp))
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    self._ckpt = ckpt
    log.debug('Flipping recovery mode off.')
    self._recovery = False
    self._set_task_status(TaskState.ACTIVE)
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
    assert self.task_state() == TaskState.ACTIVE, 'Should not resume inactive task.'
    self._initialize_ckpt_header()
    self._replay(unapplied_updates)

  def _ckpt_write(self, record):
    """
      Write to the checkpoint if we're not in recovery mode.
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

  def _run_task_state_machine(self):
    """
      Run the task state machine, returning True if a terminal state has been reached.
    """
    # State machine should only be run after checkpoint acquisition, which prepends
    # active status.
    assert self._state.statuses not in (None, [])
    if self._is_task_failed():
      self._set_task_status(TaskState.FAILED)
      return True
    elif self._planner.is_complete():
      self._set_task_status(TaskState.SUCCESS)
      return True
    return False

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
        user = self._user,
        ports = self._portmap)
      runner_ckpt = RunnerCkpt(runner_header=header)
      self._dispatcher.dispatch(self._state, runner_ckpt)

  def _set_task_status(self, state):
    update = TaskStatus(state = state, timestamp_ms = int(self._clock.time() * 1000),
                        runner_pid = os.getpid(), runner_uid = os.getuid())
    runner_ckpt = RunnerCkpt(task_status=update)
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _set_process_status(self, process_name, process_state, sequence, **kw):
    runner_ckpt = RunnerCkpt(process_status = ProcessStatus(
      process = process_name, state = process_state, seq = sequence, **kw))
    self._dispatcher.dispatch(self._state, runner_ckpt, self._recovery)

  def _get_updates_from_processes(self, timeout=None):
    STAT_INTERVAL_SLEEP = Amount(1, Time.SECONDS)
    applied_updates = 0
    total_time = 0.0
    while applied_updates == 0 and (timeout is None or total_time < timeout):
      process_updates = self._watcher.select()
      for process_update in process_updates:
        self._dispatcher.dispatch(self._state, process_update, self._recovery)
        applied_updates += 1
      total_time += STAT_INTERVAL_SLEEP.as_(Time.SECONDS)
      self._clock.sleep(STAT_INTERVAL_SLEEP.as_(Time.SECONDS))
    return applied_updates

  def _task_process_from_process_name(self, process_name, sequence_number):
    """
      Construct a Process() object from a process_name, populated with its
      correct run number and fully interpolated commandline.
    """
    run_number = len(self.state.processes[process_name])-1
    pathspec = self._pathspec.given(process = process_name, run = run_number)
    process = TaskRunnerHelper.process_from_name(self._task, process_name)
    context = ThermosContext(task_id = self._task_id,
                             user = self._user,
                             ports = self._portmap)
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
      self._sandbox,
      self._user,
      chroot=self._chroot,
      fork=close_ckpt_and_fork)

  def _is_process_failed(self, process_name):
    process = TaskRunnerHelper.process_from_name(self._task, process_name)
    def failed_run(run):
      return run.state == ProcessState.FAILED
    process_failures = Integer(len(filter(failed_run, self._state.processes.get(process_name, []))))
    log.debug('process_name: %s, process = %s, process_failures = %s' % (
      process_name, process, process_failures))
    return process.max_failures() != Integer(0) and process_failures >= process.max_failures()

  def _is_task_failed(self):
    if self._task.max_failures() == Integer(0):
      return False
    failures = filter(None, map(self._is_process_failed, self._state.processes.keys()))
    return Integer(len(failures)) >= self._task.max_failures()

  def _set_lost_task_if_necessary(self, process_name):
    """
       Determine whether or not we should mark a task as LOST and do so if necessary.
    """
    assert process_name in self._task_processes
    assert len(self._state.processes[process_name]) > 0
    current_run = self._state.processes[process_name][-1]

    def forked_but_never_came_up():
      return current_run.state == ProcessState.FORKED and (
        self._clock.time() - current_run.fork_time > TaskRunner.LOST_TIMEOUT.as_(Time.SECONDS))

    def running_but_coordinator_died():
      if current_run.state != ProcessState.RUNNING:
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

    if forked_but_never_came_up() or running_but_coordinator_died():
      log.info('Detected a LOST task: %s' % current_run)
      log.debug('  forked_but_never_came_up: %s' % forked_but_never_came_up())
      log.debug('  running_but_coordinator_died: %s' % running_but_coordinator_died())
      need_kill = current_run.pid if current_run.state is ProcessState.RUNNING else None
      self._set_process_status(process_name, ProcessState.LOST, current_run.seq + 1)
      if need_kill:
        self._ps.collect_all()
        if need_kill not in self._ps.pids():
          log.error('PID disappeared: %s' % need_kill)
        else:
          pids = [need_kill] + list(self._ps.children_of(need_kill, all=True))
          for pid in pids:
            log.info('  Killing orphaned pid: %s' % pid)
            safe_kill_pid(pid)

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
          self._set_task_status(TaskState.FAILED)
          break

        def pick_processes(process_list):
          if self._task.max_concurrency() == Integer(0):
            return process_list
          num_to_pick = max(self._task.max_concurrency().get() - len(running), 0)
          return process_list[:num_to_pick]

        for process_name in pick_processes(runnable):
          if process_name not in self._task_processes:
            self._set_process_status(process_name, ProcessState.WAITING, 0)

          log.info('Forking Process(%s)' % process_name)
          tp = self._task_processes[process_name]
          tp.start()
          launched.append(tp)

        # gather and apply state transitions
        if self._planner.get_running() or len(launched) > 0:
          if not self._get_updates_from_processes(
              timeout=TaskRunner.MAX_ITERATION_TIME.as_(Time.SECONDS)):
            # no updates to the checkpoint stream after a large wait, so touch the
            # checkpoint to prevent upstream garbage collection.
            os.utime(self._pathspec.getpath('runner_checkpoint'), None)

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

  # Need from pid handle:
  #   wall time of start
  #   user who owns the pid
  def this_is_really_our_pid(self, pid_handle, user, start_time):
    """
      A heuristic to make sure that this is likely the pid that we own/forked.  Necessary
      because of pid-space wrapping.  We don't want to go and kill processes we don't own,
      especially if the killer is running as root.
    """
    if pid_handle.user() != user:
      log.info("    Expected pid %s to be ours but the pid user is %s and we're %s" % (
        pid_handle.pid(), pid_handle.user(), user))
      return False
    estimated_start_time = self._clock.time() - pid_handle.wall_time()
    log.info("    Time drift from the start of %s is %s, real: %s, pid wall: %s, estimated: %s" % (
      pid_handle.pid(), abs(start_time - estimated_start_time), start_time, pid_handle.wall_time(),
      estimated_start_time))
    return abs(start_time - estimated_start_time) < TaskRunner.MAX_START_TIME_DRIFT.as_(
      Time.SECONDS)

  def kill(self, force=False, terminal_status=TaskState.KILLED):
    """
      Kill all processes associated with this task and set task/process states as KILLED.
    """
    assert terminal_status in (TaskState.KILLED, TaskState.LOST)
    with self.control(force):
      self._run_task_state_machine()
      if self.task_state() != TaskState.ACTIVE:
        log.warning('Task is not in ACTIVE state, cannot issue kill.')
        return
      self._set_task_status(terminal_status)

  def lose(self, force=False):
    """
      Mark a task as LOST and kill any straggling processes.
    """
    return self.kill(force, terminal_status=TaskState.LOST)

  def _kill(self):
    log.info('Killing ThermosRunner.')
    self._ps.collect_all()

    coordinator_pids = []
    process_pids = []
    process_statuses = []

    current_user = self._state.header.user
    log.info('    => Current user: %s' % current_user)
    log.debug('    => All PIDs on system: %s' % ' '.join(map(str, sorted(self._ps.pids()))))
    for process_history in self._state.processes.values():
      # collect coordinator_pids for runners in >=FORKED, <=RUNNING state
      last_run = process_history[-1]
      if last_run.state in (ProcessState.FORKED, ProcessState.RUNNING):
        process_statuses.append(last_run)
        log.info('  Inspecting %s coordinator [pid: %s]' % (last_run.process,
            last_run.coordinator_pid))
        if last_run.coordinator_pid in self._ps.pids() and self.this_is_really_our_pid(
            self._ps.get_handle(last_run.coordinator_pid), current_user, last_run.fork_time):
          coordinator_pids.append(last_run.coordinator_pid)
        else:
          log.info('    (coordinator appears to have completed)')
      if last_run.state == ProcessState.RUNNING:
        log.info('  Inspecting %s [pid: %s]' % (last_run.process, last_run.pid))
        if last_run.pid in self._ps.pids() and self.this_is_really_our_pid(
            self._ps.get_handle(last_run.pid), current_user, last_run.start_time):
          log.info('    => pid is active.')
          process_pids.append(last_run.pid)
          subtree = self._ps.children_of(last_run.pid, all=True)
          if subtree:
            log.info('      => has children: %s' % ' '.join(map(str, subtree)))
            process_pids.extend(subtree)

    pid_types = { 'Coordinator': coordinator_pids, 'Child': process_pids }
    if any(pid_types.values()):
      log.info('Issuing kills.')
    for pid_type, pid_set in pid_types.items():
      for pid in pid_set:
        log.info('  %s: %s' % (pid_type, pid))
        safe_kill_pid(pid)

    for process_status in process_statuses:
      self._set_process_status(process_status.process, ProcessState.KILLED, process_status.seq + 1,
        stop_time=self._clock.time(), return_code=-1)

    log.info('Kill complete.')
