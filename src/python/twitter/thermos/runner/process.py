from abc import ABCMeta, abstractmethod
import getpass
import grp
import os
import pwd
import signal
import subprocess
import sys
import time

from twitter.common import log
from twitter.common.dirutil import (
  lock_file,
  safe_mkdir,
  safe_open)
from twitter.common.quantity import (
  Amount,
  Time)
from twitter.common.recordio import (
  ThriftRecordReader,
  ThriftRecordWriter)

from gen.twitter.thermos.ttypes import (
  ProcessState,
  ProcessStatus,
  RunnerCkpt)

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False


class Platform(object):
  __metaclass__ = ABCMeta

  @abstractmethod
  def clock(self):
    pass

  @abstractmethod
  def fork(self):
    pass

  @abstractmethod
  def getpid(self):
    pass


class ProcessFactory(object):
  """
    Translates a (task, name) => Process object which has an .execute() command.
  """

  def __init__(self):
    pass

  def make(self, process_name, *args, **kw):
    """
      Get a reified Process object.

      Mostly just marshalls to the underlying concrete ProcessBase implementation, which takes
      a known (but complicated) set of parameters like:
         name
         cmdline
         sequence number
         pathspec
         sandbox dir
         user
    """
    pass

  def start(self, process_name):
    pass

  def release(self, process_name):
    """
      Release a process object when it is no longer necessary.
    """
    pass

  def reap(self):
    pass

  def platform(self):
    pass



class ProcessBase(object):
  """
    Encapsulate a running process for a task.
  """
  class Error(Exception): pass
  class UnknownUserError(Error): pass
  class CheckpointError(Error): pass
  class UnspecifiedSandbox(Error): pass

  CONTROL_WAIT_CHECK_INTERVAL = Amount(100, Time.MILLISECONDS)
  MAXIMUM_CONTROL_WAIT = Amount(1, Time.MINUTES)

  def __init__(self, name, cmdline, sequence, pathspec, sandbox_dir, user=None, platform=None):
    """
      required:
        name        = name of the process
        cmdline     = cmdline of the process
        sequence    = the next available sequence number for state updates
        pathspec    = TaskPath object for synthesizing path names
        sandbox_dir = the sandbox in which to run the process
        platform    = Platform providing fork, clock, getpid

      optional:
        user        = the user to run as (if unspecified, will default to current user.)
                      if specified to a user that is not the current user, you must have root access
    """
    self._name = name
    self._cmdline = cmdline
    self._pathspec = pathspec
    self._seq = sequence
    self._sandbox = sandbox_dir
    if self._sandbox:
      safe_mkdir(self._sandbox)
    self._pid = None
    self._fork_time = None
    self._stdout = None
    self._stderr = None
    self._user = user
    if self._user:
      try:
        pwd.getpwnam(self._user)
      except KeyError:
        raise ProcessBase.UnknownUserError('Unknown user %s!' % self._user)
    self._ckpt = None
    self._ckpt_head = -1
    assert platform is not None
    self._platform = platform

  def _log(self, msg):
    log.debug('[process:%5s=%s]: %s' % (self._pid, self.name(), msg))

  def _ckpt_write(self, msg):
    self._init_ckpt_if_necessary()
    self._log("child state transition [%s] <= %s" % (self.ckpt_file(), msg))
    self._ckpt.write(msg)

  def _write_process_update(self, **kw):
    process_status = ProcessStatus(**kw)
    process_status.seq = self._seq
    process_status.process = self.name()
    self._ckpt_write(RunnerCkpt(process_status=process_status))
    self._seq += 1

  def _write_initial_update(self):
    self._write_process_update(state = ProcessState.FORKED,
                               fork_time = self._fork_time,
                               coordinator_pid = self._pid)

  def cmdline(self):
    return self._cmdline

  def name(self):
    return self._name

  def pid(self):
    """pid of the coordinator"""
    return self._pid

  def rebind(self, pid, fork_time):
    """rebind Process to an existing coordinator pid without forking"""
    self._pid = pid
    self._fork_time = fork_time

  def ckpt_file(self):
    return self._pathspec.given(process = self.name()).getpath('process_checkpoint')

  def _setup_ckpt(self):
    """Set up the checkpoint: must be run on the parent."""
    self._log('initializing checkpoint file: %s' % self.ckpt_file())
    ckpt_fp = lock_file(self.ckpt_file(), "a+")
    if ckpt_fp in (None, False):
      raise ProcessBase.CheckpointError('Could not acquire checkpoint permission or lock for %s!' %
        self.ckpt_file())
    self._ckpt_head = os.path.getsize(self.ckpt_file())
    ckpt_fp.seek(self._ckpt_head)
    self._ckpt = ThriftRecordWriter(ckpt_fp)
    self._ckpt.set_sync(True)

  def _init_ckpt_if_necessary(self):
    if self._ckpt is None:
      self._setup_ckpt()

  def _wait_for_control(self):
    """Wait for control of the checkpoint stream: must be run in the child."""
    total_wait_time = Amount(0, Time.SECONDS)

    with open(self.ckpt_file(), 'r') as fp:
      fp.seek(self._ckpt_head)
      rr = ThriftRecordReader(fp, RunnerCkpt)
      while total_wait_time < ProcessBase.MAXIMUM_CONTROL_WAIT:
        ckpt_tail = os.path.getsize(self.ckpt_file())
        if ckpt_tail == self._ckpt_head:
          self._platform.clock().sleep(ProcessBase.CONTROL_WAIT_CHECK_INTERVAL.as_(Time.SECONDS))
          total_wait_time += ProcessBase.CONTROL_WAIT_CHECK_INTERVAL
          continue
        checkpoint = rr.try_read()
        if checkpoint:
          assert checkpoint.process_status
          if (checkpoint.process_status.process != self.name() or
              checkpoint.process_status.state != ProcessState.FORKED or
              checkpoint.process_status.fork_time != self._fork_time or
              checkpoint.process_status.coordinator_pid != self._pid):
            self._log('Losing control of the checkpoint stream:')
            self._log('   fork_time [%s] vs self._fork_time [%s]' % (
                checkpoint.process_status.fork_time, self._fork_time))
            self._log('   coordinator_pid [%s] vs self._pid [%s]' % (
                checkpoint.process_status.coordinator_pid, self._pid))
            raise ProcessBase.CheckpointError('Lost control of the checkpoint stream!')
          self._log('Taking control of the checkpoint stream at record: %s' %
            checkpoint.process_status)
          self._seq = checkpoint.process_status.seq + 1
          return True
    raise ProcessBase.CheckpointError('Timed out waiting for checkpoint stream!')

  def _prepare_fork(self):
    self._stdout = safe_open(self._pathspec.with_filename('stdout').getpath('process_logdir'), "w")
    self._stderr = safe_open(self._pathspec.with_filename('stderr').getpath('process_logdir'), "w")
    self._fork_time = self._platform.clock().time()
    self._setup_ckpt()

  def _finalize_fork(self):
    self._write_initial_update()
    self._ckpt.close()
    self._ckpt = None

  def start(self):
    """
      This is the main call point into the runner.

      Forks off the process specified by task/process.  The forked off child never returns,
      but the parent returns immediately and populates information about the pid of the
      process runner process.
    """
    self._prepare_fork()
    self._pid = self._platform.fork()
    if self._pid == 0:
      self._pid = self._platform.getpid()
      self._wait_for_control()
      try:
        self.execute()
      finally:
        self._ckpt.close()
        self.finish()
    else:
      self._finalize_fork()

  def execute(self):
    raise NotImplementedError

  def finish(self):
    pass


class RealPlatform(Platform):
  IGNORE_SIGNALS = (signal.SIGINT,)

  def __init__(self, fork=os.fork):
    self._fork = fork

  def fork(self):
    pid = self._fork()
    if pid == 0:
      self._sanitize()
    return pid

  def _sanitize(self):
    for sig in self.IGNORE_SIGNALS:
      signal.signal(sig, signal.SIG_IGN)

  def getpid(self):
    return os.getpid()

  def clock(self):
    return time


class Process(ProcessBase):
  """
    Encapsulate a running process for a task.
  """
  def __init__(self, *args, **kw):
    """
      See ProcessBase.__init__

      Takes additional arguments:
        fork: the fork function to use [default: os.fork]
        chroot: whether or not to chroot into the sandbox [default: False]
    """
    fork = kw.pop('fork', os.fork)
    self._use_chroot = bool(kw.pop('chroot', False))
    kw['platform'] = RealPlatform(fork=fork)
    ProcessBase.__init__(self, *args, **kw)
    if self._use_chroot and self._sandbox is None:
      raise Process.UnspecifiedSandbox('If using chroot, must specify sandbox!')

  def _chroot(self):
    """
      Chroot to the sandbox directory.
    """
    os.chdir(self._sandbox)
    os.chroot(self._sandbox)

  def _setuid(self):
    """
      Drop privileges to the user supplied in Process creation (if necessary.)
    """
    try:
      user = pwd.getpwnam(self._user)
      current_user = pwd.getpwuid(os.getuid())
    except KeyError:
      raise ProcessBase.UnknownUserError('Unable to get pwent information in order to setuid!')

    if user.pw_uid == current_user.pw_uid:
      return

    def drop_privs():
      uid, gid = user.pw_uid, user.pw_gid
      username = user.pw_name
      group_ids = [group.gr_gid for group in grp.getgrall() if username in group.gr_mem]
      os.setgroups(group_ids)
      os.setgid(gid)
      os.setuid(uid)

    def update_environment():
      username, homedir = user.pw_name, user.pw_dir
      os.unsetenv('MAIL')
      os.putenv('HOME', homedir)
      for attr in ('LOGNAME', 'USER', 'USERNAME'):
        os.putenv(attr, username)

    drop_privs()
    update_environment()

  def execute(self):
    assert self._stderr
    assert self._stdout

    # TODO(wickman) reconsider setsid now that we're invoking in a subshell
    os.setsid()
    if self._use_chroot:
      self._chroot()
    if self._user:
      self._setuid()

    # start process
    start_time = self._platform.clock().time()

    if not self._sandbox:
      sandbox = os.getcwd()
    else:
      sandbox = self._sandbox if not self._use_chroot else '/'

    self._popen = subprocess.Popen(["/bin/bash", "-c", self.cmdline()],
                                   stderr=self._stderr, stdout=self._stdout, cwd=sandbox)

    self._write_process_update(state=ProcessState.RUNNING,
                               pid=self._popen.pid,
                               start_time=start_time)

    # wait for job to finish
    rc = self._popen.wait()

    # indicate that we have finished/failed
    if rc < 0:
      state = ProcessState.KILLED
    elif rc == 0:
      state = ProcessState.SUCCESS
    else:
      state = ProcessState.FAILED

    self._write_process_update(state=state, return_code=rc,
                               stop_time=self._platform.clock().time())

  def finish(self):
    self._log('Coordinator exiting.')
    sys.exit(0)
