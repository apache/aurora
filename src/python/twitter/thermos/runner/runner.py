import os
import copy
import time
import random
import socket

from twitter.common import log
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from twitter.thermos.base.helper   import Helper
from twitter.thermos.base.path     import WorkflowPath
from twitter.thermos.base.ckpt     import WorkflowCkptDispatcher
from twitter.thermos.runner.chroot   import WorkflowChroot
from twitter.thermos.runner.planner  import Planner
from twitter.thermos.runner.task     import WorkflowTask
from twitter.thermos.runner.muxer    import WorkflowTaskMuxer
from twitter.thermos.runner.ports    import EphemeralPortAllocator

from twitter.tcl.scheduler import Scheduler

# thermos_internal.thrift
from tcl_thrift.ttypes import ThermosJobHeader
from thermos_thrift.ttypes import *

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False
__todo__   = """
  Implement active/finished ThermosJob dump handling.
"""

class WorkflowRunner_InternalError(Exception): pass
class WorkflowRunner_NotImplementedException(Exception): pass

class WorkflowRunnerHelper:
  @staticmethod
  def scheduler_from_workflow(workflow):
    scheduler = Scheduler()
    for task in workflow.tasks:
      scheduler.add(task.name)
    for execution_dependency in workflow.taskBefores:
      scheduler.run_before(execution_dependency.first, execution_dependency.second)
    for task_set in workflow.taskTogethers:
      scheduler.run_set_together(task_set.tasks)
    return scheduler

class WorkflowRunner:
  def __init__(self, workflow, sandbox, root_dir, job_uid):
    """
      workflow = ThermosWorkflow to run
      sandbox  = sandbox in which to run all tasks (pathname)
      root_dir = directory to store all log/ckpt data (pathname)
      job_uid  = uid assigned to the parent job, used to disambiguate cached checkpoints
    """
    self._workflow = copy.deepcopy(workflow)
    self._workflow.job.uid = job_uid
    self._workflow_tasks = {}
    self._watcher  = WorkflowTaskMuxer()
    if self._workflow.replicaId is None:
      raise Exception("Workflow must have a replica_id!")
    self._pathspec = WorkflowPath(root = root_dir, job_uid = job_uid)
    self._recovery = True  # set in recovery mode
    self._port_allocator = EphemeralPortAllocator()

    if self._has_finished_job() is not None:
      log.info('Already have a finished job.')
      self.die()
      return

    # create scheduler from task workflow
    scheduler = WorkflowRunnerHelper.scheduler_from_workflow(workflow)
    # create planner from scheduler
    self._planner = Planner(scheduler)

    # set up sandbox for running process
    self._sandbox = WorkflowChroot(sandbox, job_uid)
    self._sandbox.create_if_necessary()

    # create runner state
    self._state      = WorkflowRunnerState(tasks = {})
    self._dispatcher = WorkflowCkptDispatcher()
    self._register_handlers()

    # recover checkpointed state and update plan
    self._setup_checkpointed_state(self._pathspec.getpath('runner_checkpoint'))

    # for any workflow task that hasn't yet been initialized into ._state, initialize
    self._update_runner_ckpt_to_high_watermarks()
    self._initialize_tasks()

  def _setup_checkpointed_state(self, ckpt_file):
    """ give it: ckpt filename, state object
        returns: ckpt recordwriter, mutates state accordingly
    """
    # recover if necessary
    if os.path.exists(ckpt_file):
      fp = file(ckpt_file, "r")
      ckpt_recover = ThriftRecordReader(fp, WorkflowRunnerCkpt)
      for wrc in ckpt_recover:
        self._dispatcher.update_runner_state(self._state, wrc, recovery = self._recovery)
      ckpt_recover.close()

    fp = Helper.safe_create_file(ckpt_file, "a")
    ckpt = ThriftRecordWriter(fp)
    ckpt.set_sync(True)
    self._ckpt = ckpt

  def _initialize_tasks(self):
    if self._state.header is None:
      update = WorkflowRunnerHeader(
        job_name          = self._workflow.job.name,
        job_uid           = self._workflow.job.uid,
        workflow_name     = self._workflow.name,
        workflow_replica  = self._workflow.replicaId,
        launch_time       = long(time.time()),
        hostname          = socket.gethostname())
      runner_ckpt = WorkflowRunnerCkpt(runner_header = update)
      self._dispatcher.update_runner_state(self._state, runner_ckpt)
      self._ckpt.write(runner_ckpt)  # change this if we add dispatches for non-task updates
      self._set_workflow_state(WorkflowState.ACTIVE)

    for task in self._workflow.tasks:
      if task.name not in self._state.tasks:
         update = WorkflowTaskState(task = task.name, seq = 0, run_state = WorkflowTaskRunState.WAITING)
         runner_ckpt = WorkflowRunnerCkpt(task_state = update)
         self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery)

  def _set_workflow_state(self, state):
    update = WorkflowStateUpdate(state = state)
    runner_ckpt = WorkflowRunnerCkpt(state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery):
      self._ckpt.write(runner_ckpt)

  def _set_task_history_state(self, task, state):
    update = WorkflowRunStateUpdate(task = task, state = state)
    runner_ckpt = WorkflowRunnerCkpt(history_state_update = update)
    if self._dispatcher.update_runner_state(self._state, runner_ckpt, recovery = self._recovery):
      self._ckpt.write(runner_ckpt)

  # task transitions for the state machine
  def _on_everything(self, task_update):
    if not self._recovery:
      self._ckpt.write(WorkflowRunnerCkpt(task_state = task_update))

  def _on_waiting (self, task_update):
    log.debug('_on_waiting %s' % task_update)
    self._workflow_tasks[task_update.task] = self._workflow_task_from_task_name(task_update.task)
    self._watcher.register(self._workflow_tasks[task_update.task])
    self._planner.forget(task_update.task)

  def _on_forked  (self, task_update):
    log.debug('_on_forked %s' % task_update)
    wf_task = self._workflow_tasks[task_update.task]
    wf_task.set_fork_time(task_update.fork_time)
    wf_task.set_pid(task_update.runner_pid)
    self._planner.set_running(task_update.task)

  def _on_running (self, task_update):
    log.debug('_on_running %s' % task_update)
    self._planner.set_running(task_update.task)

  def _on_finished(self, task_update):
    log.debug('_on_finished %s' % task_update)
    self._workflow_tasks.pop(task_update.task)
    self._watcher.unregister(task_update.task)
    self._planner.set_finished(task_update.task)
    self._set_task_history_state(task_update.task, WorkflowRunState.SUCCESS)

  def _on_abnormal(self, task_update):
    self._workflow_tasks.pop(task_update.task)
    self._watcher.unregister(task_update.task)
    self._on_waiting(task_update)

  def _on_failed  (self, task_update):
    log.debug('_on_failed %s' % task_update)
    self._on_abnormal(task_update)

  def _on_lost    (self, task_update):
    log.debug('_on_lost %s' % task_update)
    self._on_abnormal(task_update)

  def _on_port_allocation(self, name, port):
    self._port_allocator.allocate_port(name, port)

  def _register_handlers(self):
    self._dispatcher.register_universal_handler(lambda u: self._on_everything(u))
    self._dispatcher.register_state_handler(WorkflowTaskRunState.WAITING,  lambda u: self._on_waiting(u))
    self._dispatcher.register_state_handler(WorkflowTaskRunState.FORKED,   lambda u: self._on_forked(u))
    self._dispatcher.register_state_handler(WorkflowTaskRunState.RUNNING,  lambda u: self._on_running(u))
    self._dispatcher.register_state_handler(WorkflowTaskRunState.FAILED,   lambda u: self._on_failed(u))
    self._dispatcher.register_state_handler(WorkflowTaskRunState.FINISHED, lambda u: self._on_finished(u))
    self._dispatcher.register_state_handler(WorkflowTaskRunState.LOST,     lambda u: self._on_lost(u))
    self._dispatcher.register_port_handler(lambda name, port: self._on_port_allocation(name, port))

  def _update_runner_ckpt_to_high_watermarks(self):
    task_updates = self._watcher.select()
    unapplied_task_updates = []
    for task_update in task_updates:
      if self._dispatcher.would_update(self._state, task_update):
        unapplied_task_updates.append(task_update)
      else:
        self._dispatcher.update_runner_state(self._state, task_update, recovery = True)

    self._recovery = False
    for task_update in unapplied_task_updates:
      assert self._dispatcher.update_runner_state(self._state, task_update)

  def _get_updates_from_tasks(self):
    applied_updates = 0
    while applied_updates == 0:
      task_updates = self._watcher.select()
      for task_update in task_updates:
        if self._dispatcher.update_runner_state(self._state, task_update):
          applied_updates += 1

      # this is crazy -- wtf is select returning instantly?
      # make this OsController.tick() so we can mock out some of this stuff
      # e.g. select.select() and such. -- or write our own OS agnostic FdSelect class
      time.sleep(0.05)

  def _write_job(self):
    active_job_path = self._pathspec.getpath('active_job_path')
    fp = Helper.safe_create_file(active_job_path, 'w')
    rw = ThriftRecordWriter(fp)
    rw.write(self._workflow.job)
    fp.close()

  def _has_finished_job(self):
    finished_job_path = self._pathspec.getpath('finished_job_path')
    if not os.path.exists(finished_job_path): return None
    fp = file(finished_job_path, "r")
    rr = ThriftRecordReader(fp, ThermosJobHeader)
    job = rr.read()
    fp.close()
    if job is None: return None
    return job == self._workflow.job

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
        if self._workflow.job != job:
          raise WorkflowRunner_InternalError("Attempting to launch different jobs with same uid?")

  def _save_allocated_ports(self, ports):
    for name in ports:
      wap = WorkflowAllocatedPort(port_name = name, port = ports[name])
      runner_ckpt = WorkflowRunnerCkpt(allocated_port = wap)
      if self._dispatcher.update_runner_state(self._state, runner_ckpt, self._recovery):
        self._ckpt.write(runner_ckpt)

  def _workflow_task_from_task_name(self, task_name):
    pathspec = self._pathspec.given(task = task_name, run = Helper.task_run_number(self._state, task_name))
    task = Helper.task_from_workflow(self._workflow, task_name)
    (new_cmdline, allocated_ports) = self._port_allocator.synthesize(task.commandLine)
    log.info('allocated ports: %s' % allocated_ports)
    self._save_allocated_ports(allocated_ports)
    task.commandLine = new_cmdline
    return WorkflowTask(pathspec,
                        task,
                        Helper.task_sequence_number(self._state, task_name),
                        self._sandbox.path())

  def _is_task_failed(self, task_name):
    task = Helper.task_from_name(self._workflow, task_name)
    task_failures = filter(lambda run: run.run_state == WorkflowTaskRunState.FAILED,
                           self._state.tasks[task.name].runs)
    return task.maxFailures != 0 and len(task_failures) >= task.maxFailures

  def _is_workflow_failed(self):
    failures = filter(lambda history: history.state == WorkflowState.FAILED,
                      self._state.tasks.values())
    return self._workflow.maxFailures != 0 and len(failures) >= self._workflow.maxFailures

  def run(self):
    if self._has_finished_job() is not None:
      log.info('Run short-circuiting.')
      return
    else:
      log.info('Run not short-circuiting: has_finished_job = %s' % self._has_finished_job())

    # out of recovery mode
    if not self._sandbox.created():
      raise WorkflowRunner_InternalError("Sandbox not created before start() called.")

    MIN_ITER_TIME = 0.1

    while True:
      log.info('------')

      time_now = time.time()

      if self._is_workflow_failed():
        self._set_workflow_state(WorkflowState.FAILED)
        self.kill()
        return

      if self._planner.is_complete():
        self._set_workflow_state(WorkflowState.SUCCESS)
        break

      # make sure Job is living in 'active' state
      self._enforce_job_active()

      # do scheduling run
      runnable = list(self._planner.get_runnable())
      log.info('runnable: %s' % ' '.join(runnable))
      log.info('finished: %s' % ' '.join(list(self._planner.finished)))
      log.info('running:  %s' % ' '.join(list(self._planner.running)))

      scheduled = False
      for task_name in runnable:
        if self._is_task_failed(task_name):
          log.warning('Task failed: %s' % task_name)
          self._set_task_history_state(task_name, WorkflowRunState.FAILED)
          self._planner.set_finished(task_name)
          continue

        log.info('Forking WorkflowTask(%s)' % task_name)
        wt = self._workflow_tasks[task_name]
        wt.fork()
        scheduled = True

      # gather and apply state transitions
      if self._planner.get_running() or scheduled:
        self._get_updates_from_tasks()

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
