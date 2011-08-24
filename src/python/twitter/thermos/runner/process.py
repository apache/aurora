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

class Process_OwnerException(Exception): pass

class Process(object):
  """
    Encapsulate a running process for a task.
  """

  def __init__(self, pathspec, process, sequence_number, sandbox_dir):
    """
      pathspec    = TaskPath object for synthesizing path names
      process     = the process in the ThermosTask thrift blob to run
      sequence    = the current sequence number for ProcessState updates
      sandbox_dir = the sandbox in which to run the process
    """
    self._process   = process
    self._pathspec  = pathspec
    self._stdout    = pathspec.with_filename('stdout').getpath('process_logdir')
    self._stderr    = pathspec.with_filename('stderr').getpath('process_logdir')
    self._seq       = sequence_number + 1
    self._owner     = False
    self._sandbox   = sandbox_dir
    self._pid       = None
    self._fork_time = None
    self._initial_update = None

    # TODO(wickman)  Use twitter.common.dirutil
    # make sure the sandbox dir has been created
    try:
      os.makedirs(self._sandbox)
    except OSError, e:
      if e.errno != errno.EEXIST:
        raise

  def _log(self, msg):
    log.debug('[process:%5s=%s]: %s' % (self._pid, self._process.name, msg))

  def _write_process_update(self, runner_ckpt):
    # indicate that we are running
    self._seq += 1
    runner_ckpt.process_state.seq  = self._seq
    runner_ckpt.process_state.process = self._process.name

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
    # TODO(wickman)  Use properly descriptive variables and not wts/wrc
    run_state = ProcessRunState.FINISHED if (rc == 0) else ProcessRunState.FAILED
    wts = ProcessState(run_state = run_state, return_code = rc, stop_time = time.time())
    wrc = TaskRunnerCkpt(process_state = wts)
    self._write_process_update(wrc)

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

    # TODO(wickman) parameterize this.
    # break away from our parent
    os.setsid()

    # start process process
    self._start_time = time.time()
    self._popen = subprocess.Popen(["/bin/sh", "-c", self._process.cmdline],
                     stderr = self._stderr_fd,
                     stdout = self._stdout_fd,
                     cwd    = self._sandbox)
    self._process_pid = self._popen.pid

    wts = ProcessState(run_state = ProcessRunState.RUNNING,
      pid = self._process_pid, start_time = self._start_time)
    wrc = TaskRunnerCkpt(process_state = wts)
    self._write_process_update(wrc)

    # wait until finished
    self.wait()

  # Process.fork() returns in parent process, does not return in child process.
  def fork(self):
    """
      This is the main call point into the runner.

      Forks off the process specified by task/process.  The forked off child never returns,
      but the parent returns immediately and populates information about the pid of the
      process runner process.
    """
    self._fork_time = time.time()
    self._pid       = os.fork()
    self._owner     = (self._pid == 0)
    if self._owner:
      self._pid = os.getpid()
      self._real_fork()
    self._set_initial_update()

  def _set_initial_update(self):
    initial_update = ProcessState(seq = self._seq,
      process    = self._process.name,
      run_state  = ProcessRunState.FORKED,
      fork_time  = self._fork_time,
      runner_pid = self._pid)
    self._initial_update = TaskRunnerCkpt(process_state = initial_update)

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
  # should we be feeding it ProcessStates?
  def set_pid(self, pid):
    self._pid = pid

  def fork_time(self):
    return self._fork_time

  def set_fork_time(self, fork_time):
    self._fork_time = fork_time

  def name(self):
    return self._process.name

  def ckpt_file(self):
    return self._pathspec.given(pid = self._pid).getpath('process_checkpoint')

  def die(self):
    self._log('die() called, sending SIGKILL to children.')
    if self.running():
      self._popen.kill()
    else:
      self._log('WARNING: children already dead.')
    sys.exit(1)
