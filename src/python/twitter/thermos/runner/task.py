import os
import errno
import time
import subprocess

from twitter.common import log
from twitter.common.recordio import ThriftRecordWriter

from thermos_thrift.ttypes import *
from twitter.thermos.base.helper   import Helper

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class WorkflowTask_OwnerException(Exception): pass

class WorkflowTask:
  """
    Encapsulate a running workflow task.
  """

  def __init__(self, pathspec, task, sequence_number, sandbox_dir):
    """
      pathspec    = WorkflowPath object for synthesizing path names
      task        = the task in the ThermosWorkflow thrift blob to run
      sequence    = the current sequence number for WorkflowTaskState updates
      sandbox_dir = the sandbox in which to run the task
    """
    self._task      = task
    self._pathspec  = pathspec
    self._stdout    = pathspec.with_filename('stdout').getpath('task_logdir')
    self._stderr    = pathspec.with_filename('stderr').getpath('task_logdir')
    self._seq       = sequence_number + 1
    self._owner     = False
    self._sandbox   = sandbox_dir
    self._pid       = None
    self._fork_time = None
    self._initial_update = None

    # make sure the sandbox dir has been created
    try:
      os.makedirs(self._sandbox)
    except OSError, e:
      if e.errno != errno.EEXIST:
        raise

  def _log(self, msg):
    log.debug('[task:%5s=%s]: %s' % (self._pid, self._task.name, msg))

  def _write_task_update(self, runner_ckpt):
    # indicate that we are running
    self._seq += 1
    runner_ckpt.task_state.seq  = self._seq
    runner_ckpt.task_state.task = self._task.name

    self._log("child state transition [%s] <= %s" % (self.ckpt_file(), runner_ckpt))
    if not ThriftRecordWriter.append(self.ckpt_file(), runner_ckpt):
      self._log("failed to write status, dying.")
      self.die()

  def wait(self):
    assert self._owner

    # wait for job to finish
    self._popen.wait()
    rc = self._popen.returncode

    # indicate that we have finished/failed
    run_state = WorkflowTaskRunState.FINISHED if (rc == 0) else WorkflowTaskRunState.FAILED
    wts = WorkflowTaskState(run_state = run_state, return_code = rc, stop_time = time.time())
    wrc = WorkflowRunnerCkpt(task_state = wts)
    self._write_task_update(wrc)

    # normal exit
    sys.exit(0)

  def _real_fork(self):
    assert self._owner

    # open file handles for child process
    self._stdout_fd = Helper.safe_create_file(self._stdout, "w")
    self._stderr_fd = Helper.safe_create_file(self._stderr, "w")

    # create file and zero it out
    fp = Helper.safe_create_file(self.ckpt_file(), "w")
    fp.close()

    # break away from our parent
    os.setsid()

    # start task process
    self._start_time = time.time()
    self._popen = subprocess.Popen(["/bin/sh", "-c", self._task.commandLine],
                     stderr = self._stderr_fd,
                     stdout = self._stdout_fd,
                     cwd    = self._sandbox)
    self._task_pid = self._popen.pid

    wts = WorkflowTaskState(run_state = WorkflowTaskRunState.RUNNING,
                            pid = self._task_pid, start_time = self._start_time)
    wrc = WorkflowRunnerCkpt(task_state = wts)
    self._write_task_update(wrc)

    # wait until finished
    self.wait()

  # WorkflowTask.fork() returns in parent process, does not return in child process.
  def fork(self):
    """
      This is the main call point into the runner.

      Forks off the task specified by workflow/task.  The forked off child never returns,
      but the parent returns immediately and populates information about the pid of the
      task runner process.
    """
    self._fork_time = time.time()
    self._pid       = os.fork()
    self._owner     = (self._pid == 0)
    if self._owner:
      self._pid = os.getpid() # get true pid
      self._real_fork()
    self._set_initial_update()

  def _set_initial_update(self):
    initial_update = WorkflowTaskState(seq        = self._seq,
                                       task       = self._task.name,
                                       run_state  = WorkflowTaskRunState.FORKED,
                                       fork_time  = self._fork_time,
                                       runner_pid = self._pid)
    self._initial_update = WorkflowRunnerCkpt(task_state = initial_update)

  def initial_update(self):
    update = self._initial_update
    self._initial_update = None
    return update

  def running(self):
    if self._popen: self._popen.poll()
    if self._popen is None: return True
    if self._popen.returncode is None: return True
    return False

  def rc(self):
    if self.running(): return None
    return self._popen.returncode

  # this is the pid of the runner
  def pid(self):
    return self._pid

  # this is ONLY for recovery. is there a better way to do this?
  # should we be feeding it WorkflowTaskStates?
  def set_pid(self, pid):
    self._pid = pid

  def fork_time(self):
    return self._fork_time

  def set_fork_time(self, fork_time):
    self._fork_time = fork_time

  def name(self):
    return self._task.name

  def ckpt_file(self):
    return self._pathspec.given(pid = self._pid).getpath('task_checkpoint')

  def die(self):
    self._log('die() called, sending SIGKILL to children.')
    if self.running():
      self._popen.kill()
    else:
      self._log('WARNING: children already dead.')
    sys.exit(1)
