import os
import copy
import time
import random
import socket

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from twitter.thermos.base.helper    import Helper
from twitter.thermos.base.path      import TaskPath
from twitter.thermos.base.ckpt      import TaskCkptDispatcher
from twitter.thermos.runner.chroot  import TaskChroot
from twitter.thermos.runner.planner import Planner
from twitter.thermos.runner.process import Process
from twitter.thermos.runner.muxer   import ProcessMuxer
from twitter.thermos.runner.ports   import EphemeralPortAllocator

from twitter.tcl.scheduler import Scheduler

from tcl_thrift.ttypes import ThermosJobHeader
from thermos_thrift.ttypes import *

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False
__todo__   = """
  Implement active/finished ThermosJob dump handling.
"""

class TaskRunnerHelper(object):
  @staticmethod
  def scheduler_from_task(task):
    scheduler = Scheduler()
    for process in task.processes:
      scheduler.add(process.name)
    for execution_dependency in task.before_constraints:
      scheduler.run_before(execution_dependency.first, execution_dependency.second)
    for process_set in task.together_constraints:
      scheduler.run_set_together(process_set.processes)
    return scheduler

class TaskRunner(object):
  class InternalError(Exception): pass

  def __init__(self, task, sandbox, root_dir, job_uid):
    """
      task     = ThermosTask to run
      sandbox  = sandbox in which to run all processes (pathname)
      root_dir = directory to store all log/ckpt data (pathname)
      job_uid  = uid assigned to the parent job, used to disambiguate cached checkpoints
    """
    self._task = copy.deepcopy(task)
    self._task.job.uid = job_uid
    self._task_processes = {}
    self._watcher = ProcessMuxer()
    if self._task.replica_id is None:
      raise Exception("Task must have a replica_id!")
    self._pathspec = TaskPath(root = root_dir, job_uid = job_uid)
    self._recovery = True  # set in recovery mode
    self._port_allocator = EphemeralPortAllocator()

    if self._has_finished_job() is not None:
      log.info('Already have a finished job.')
      self.die()
      return

    # create scheduler from process task
    scheduler = TaskRunnerHelper.scheduler_from_task(task)
    # create planner from scheduler
    self._planner = Planner(scheduler)

    # set up sandbox for running process
    self._sandbox = TaskChroot(sandbox, job_uid)
    self._sandbox.create_if_necessary()

    # create runner state
    self._state      = TaskRunnerState(processes = {})
    self._dispatcher = TaskCkptDispatcher()
    self._register_handlers()

    # recover checkpointed state and update plan
    self._setup_checkpointed_state(self._pathspec.getpath('runner_checkpoint'))

    # for any task process that hasn't yet been initialized into ._state, initialize
    self._update_runner_ckpt_to_high_watermarks()
    self._initialize_processes()

  def _setup_checkpointed_state(self, ckpt_file):
    """ give it: ckpt filename, state object
        returns: ckpt recordwriter, mutates state accordingly
    """
    # recover if necessary
    if os.path.exists(ckpt_file):
      fp = file(ckpt_file, "r")
      ckpt_recover = ThriftRecordReader(fp, TaskRunnerCkpt)
      for wrc in ckpt_recover:
        self._dispatcher.update_runner_state(self._state, wrc, recovery = self._recovery)
      ckpt_recover.close()

    fp = Helper.safe_create_file(ckpt_file, "a")
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    self._ckpt = ckpt

  def _initialize_processes(self):
    if self._state.header is None:
      update = TaskRunnerHeader(
        job_name      = self._task.job.name,
        job_uid       = self._task.job.uid,
        task_name     = self._task.name,
        task_replica  = self._task.replica_id,
        launch_time   = long(time.time()),
        hostname      = socket.gethostname())
      runner_ckpt = TaskRunnerCkpt(runner_header = update)
      self._dispatcher.update_runner_state(self._state, runner_ckpt)
      self._ckpt.write(runner_ckpt)  # change this if we add dispatches for non-process updates
      self._set_task_state(TaskState.ACTIVE)

    for process in self._task.processes:
      if process.name not in self._state.processes:
         update = ProcessState(process = process.name, seq = 0, run_state = ProcessRunState.WAITING)
         runner_ckpt = TaskRunnerCkpt(process_state = update)
         self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery)

  def _set_task_state(self, state):
    update = TaskStateUpdate(state = state)
    runner_ckpt = TaskRunnerCkpt(state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery):
      self._ckpt.write(runner_ckpt)

  def _set_process_history_state(self, process, state):
    update = TaskRunStateUpdate(process = process, state = state)
    runner_ckpt = TaskRunnerCkpt(history_state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery):
      self._ckpt.write(runner_ckpt)

  # process transitions for the state machine
  def _on_everything(self, process_update):
    if not self._recovery:
      self._ckpt.write(TaskRunnerCkpt(process_state = process_update))

  def _on_waiting(self, process_update):
    log.debug('_on_waiting %s' % process_update)
    self._task_processes[process_update.process] = self._task_process_from_process_name(process_update.process)
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

  def _on_finished(self, process_update):
    log.debug('_on_finished %s' % process_update)
    self._task_processes.pop(process_update.process)
    self._watcher.unregister(process_update.process)
    self._planner.set_finished(process_update.process)
    self._set_process_history_state(process_update.process, TaskRunState.SUCCESS)

  def _on_abnormal(self, process_update):
    self._task_processes.pop(process_update.process)
    self._watcher.unregister(process_update.process)
    self._on_waiting(process_update)

  def _on_failed(self, process_update):
    log.debug('_on_failed %s' % process_update)
    self._on_abnormal(process_update)

  def _on_lost(self, process_update):
    log.debug('_on_lost %s' % process_update)
    self._on_abnormal(process_update)

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

  def _get_updates_from_processes(self):
    applied_updates = 0
    while applied_updates == 0:
      process_updates = self._watcher.select()
      for process_update in process_updates:
        if self._dispatcher.update_runner_state(self._state, process_update):
          applied_updates += 1

      # this is crazy, but necessary until we support inotify and fsevents.
      # make this OsController.tick() so we can mock out some of this stuff
      # e.g. select.select() and such. -- or write our own OS agnostic FdSelect class
      time.sleep(0.05)

  def _write_job(self):
    active_job_path = self._pathspec.getpath('active_job_path')
    fp = Helper.safe_create_file(active_job_path, 'w')
    rw = ThriftRecordWriter(fp)
    rw.write(self._task.job)
    fp.close()

  def _has_finished_job(self):
    finished_job_path = self._pathspec.getpath('finished_job_path')
    if not os.path.exists(finished_job_path): return None
    fp = file(finished_job_path, "r")
    rr = ThriftRecordReader(fp, ThermosJobHeader)
    job = rr.read()
    fp.close()
    if job is None: return None
    return job == self._task.job

  def _enforce_job_active(self):
    # make sure Job is living in 'active' state
    active_job_path = self._pathspec.getpath('active_job_path')

    if not os.path.exists(active_job_path):
      self._write_job()
    else:
      # make sure it's the same
      fp = file(active_job_path, "r")
      rr = ThriftRecordReader(fp, ThermosJobHeader)
      job = rr.read()
      fp.close()
      if job is None:
        log.error('Corrupt job detected! %s, overwriting...' % active_job_path)
        self._write_job()
      else:
        if self._task.job != job:
          raise TaskRunner.InternalError("Attempting to launch different jobs with same uid?")

  def _save_allocated_ports(self, ports):
    for name in ports:
      wap = TaskAllocatedPort(port_name = name, port = ports[name])
      runner_ckpt = TaskRunnerCkpt(allocated_port = wap)
      if self._dispatcher.update_runner_state(self._state, runner_ckpt, self._recovery):
        self._ckpt.write(runner_ckpt)

  def _task_process_from_process_name(self, process_name):
    pathspec = self._pathspec.given(
      process = process_name, run = Helper.process_run_number(self._state, process_name))
    process = Helper.process_from_task(self._task, process_name)
    (new_cmdline, allocated_ports) = self._port_allocator.synthesize(process.cmdline)
    log.info('allocated ports: %s' % allocated_ports)
    self._save_allocated_ports(allocated_ports)
    process.cmdline = new_cmdline
    return Process(
      pathspec, process,
      Helper.process_sequence_number(self._state, process_name),
      self._sandbox.path())

  def _is_process_failed(self, process_name):
    process = Helper.process_from_name(self._task, process_name)
    process_failures = filter(
      lambda run: run.run_state == ProcessRunState.FAILED,
      self._state.processes[process.name].runs)
    return process.max_process_failures != 0 and (
      len(process_failures) >= process.max_process_failures)

  def _is_task_failed(self):
    failures = filter(
      lambda history: history.state == TaskState.FAILED,
      self._state.processes.values())
    return self._task.max_process_failures != 0 and (
      len(failures) >= self._task.max_process_failures)

  def run(self):
    if self._has_finished_job() is not None:
      log.info('Run short-circuiting.')
      return
    else:
      log.info('Run not short-circuiting: has_finished_job = %s' % self._has_finished_job())

    # out of recovery mode
    if not self._sandbox.created():
      raise TaskRunner.InternalError("Sandbox not created before start() called.")

    MIN_ITER_TIME = 0.1

    while True:
      log.info('------')

      time_now = time.time()

      if self._is_task_failed():
        self._set_task_state(TaskState.FAILED)
        self.kill()
        return

      if self._planner.is_complete():
        self._set_task_state(TaskState.SUCCESS)
        break

      # make sure Job is living in 'active' state
      self._enforce_job_active()

      # do scheduling run
      runnable = list(self._planner.get_runnable())
      log.info('runnable: %s' % ' '.join(runnable))
      log.info('finished: %s' % ' '.join(list(self._planner.finished)))
      log.info('running:  %s' % ' '.join(list(self._planner.running)))

      scheduled = False
      for process_name in runnable:
        if self._is_process_failed(process_name):
          log.warning('Process failed: %s' % process_name)
          self._set_process_history_state(process_name, TaskRunState.FAILED)
          self._planner.set_finished(process_name)
          continue

        log.info('Forking Process(%s)' % process_name)
        wt = self._task_processes[process_name]
        wt.fork()
        scheduled = True

      # gather and apply state transitions
      if self._planner.get_running() or scheduled:
        self._get_updates_from_processes()

    self.cleanup()
    return

  def die(self):
    pass

  def kill(self):
    log.warning('Need to implement kill()!')
    self.cleanup()
    pass

  def cleanup(self):
    # do stuff here
    self._ckpt.close()

    # make sure Job is living in 'active' state
    active_job_path   = self._pathspec.getpath('active_job_path')
    finished_job_path = self._pathspec.getpath('finished_job_path')
    active_exists     = os.path.exists(active_job_path)
    finished_exists   = os.path.exists(finished_job_path)

    # XXX Do some error handling here?
    if active_exists and not finished_exists:
      # yay!
      Helper.safe_create_dir(os.path.dirname(finished_job_path))
      os.rename(active_job_path, finished_job_path)
    else:
      # fuck!
      log.error('WARNING: active_exists: %s, finished_exists: %s' % (active_exists, finished_exists))
