import getpass
import grp
import os
import pwd
import subprocess
import sys
import time

from twitter.common import log
from twitter.common.dirutil import safe_mkdir, safe_open
from twitter.common.recordio import ThriftRecordWriter

from gen.twitter.thermos.ttypes import (
  ProcessRunState,
  ProcessState,
  RunnerCkpt)

__author__ = 'wickman@twitter.com (brian wickman)'
__tested__ = False

class Process(object):
  """
    Encapsulate a running process for a task.
  """
  class UnknownUserError(Exception): pass

  def __init__(self, name,
                     cmdline,
                     sequence_number,
                     pathspec,
                     sandbox_dir,
                     user=None,
                     chroot=False,
                     fork=os.fork):
    """
      required:
        name        = name of the process
        cmdline     = cmdline of the process
        sequence    = the current sequence number for ProcessState updates
        pathspec    = TaskPath object for synthesizing path names
        sandbox_dir = the sandbox in which to run the process

      optional:
        user        = If specified, run as this user (requires to be run as superuser.)
        chroot      = If specified, run chrooted to the sandbox.
        fork        = The fork function to use for forking (e.g. a wrapper around fork to
                      release locks after forking.)
    """
    self._name = name
    self._cmdline = cmdline
    self._pathspec = pathspec
    self._stdout = pathspec.with_filename('stdout').getpath('process_logdir')
    self._stderr = pathspec.with_filename('stderr').getpath('process_logdir')
    self._seq = sequence_number + 1
    self._owner = False
    self._sandbox = sandbox_dir
    safe_mkdir(self._sandbox)
    self._pid = None
    self._fork_time = None
    self._initial_update = None
    self._ckpt_writer = None
    self._stdout_fd = None
    self._stderr_fd = None
    self._user = user
    self._fork = fork
    if self._user:
      try:
        pwd.getpwnam(self._user)
      except KeyError:
        raise Process.UnknownUserError('Unknown user %s!' % self._user)
    self._use_chroot = bool(chroot)

  def _log(self, msg):
    log.debug('[process:%5s=%s]: %s' % (self._pid, self.name(), msg))

  def __str__(self):
    return 'Process(%s, seq:%s, pid:%s, stdout:%s, ckpt:%s)' % (
      self.name(),
      self._seq,
      self._pid,
      self._stdout,
      'None' if self._pid is None else self.ckpt_file())

  def _write_process_update(self, runner_ckpt):
    assert self._ckpt_writer
    self._seq += 1
    runner_ckpt.process_state.seq  = self._seq
    runner_ckpt.process_state.process = self.name()
    self._log("child state transition [%s] <= %s" % (self.ckpt_file(), runner_ckpt))
    if not self._ckpt_writer.write(runner_ckpt):
      self._log("failed to write status, dying.")
      self.die()

  def _chroot(self):
    """
      Chroot to the sandbox directory.
    """
    os.chdir(self._sandbox)
    os.chroot(self._sandbox)

  def _setuid(self):
    """
      Drop privileges to the user supplied in Process creation.
    """
    user = pwd.getpwnam(self._user)

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

  def _exec(self):
    assert self._owner
    assert self._stderr_fd
    assert self._stdout_fd

    # create checkpoint file and zero it out before we chroot/setuid
    self._log('initializing checkpoint file: %s' % self.ckpt_file())
    ckpt_fp = safe_open(self.ckpt_file(), "w")
    self._ckpt_writer = ThriftRecordWriter(ckpt_fp)
    self._ckpt_writer.set_sync(True)

    # TODO(wickman) reconsider setsid now that we're invoking in a subshell
    os.setsid()
    if self._use_chroot:
      self._chroot()
    if self._user and self._user != getpass.getuser():
      self._setuid()

    # start process
    self._start_time = time.time()
    self._popen = subprocess.Popen(["/bin/sh", "-c", self.cmdline()],
                     stderr = self._stderr_fd,
                     stdout = self._stdout_fd,
                     cwd    = self._sandbox if not self._use_chroot else '/')
    self._process_pid = self._popen.pid

    wts = ProcessState(run_state = ProcessRunState.RUNNING,
      pid = self._process_pid, start_time = self._start_time)
    wrc = RunnerCkpt(process_state = wts)
    self._write_process_update(wrc)

    # wait for job to finish
    self._popen.wait()
    rc = self._popen.returncode

    # indicate that we have finished/failed
    run_state = ProcessRunState.FINISHED if (rc == 0) else ProcessRunState.FAILED
    runner_ckpt = RunnerCkpt(process_state = ProcessState(
      run_state = run_state, return_code = rc, stop_time = time.time()))
    self._write_process_update(runner_ckpt)

    # normal exit
    sys.exit(0)

  def fork(self):
    """
      This is the main call point into the runner.

      Forks off the process specified by task/process.  The forked off child never returns,
      but the parent returns immediately and populates information about the pid of the
      process runner process.
    """
    # open file handles for child process (potentially out of the chroot)
    self._stdout_fd = safe_open(self._stdout, "w")
    self._stderr_fd = safe_open(self._stderr, "w")
    self._fork_time = time.time()
    self._pid       = self._fork()
    self._owner     = (self._pid == 0)
    if self._owner:
      self._pid = os.getpid()
      self._exec()
    self._set_initial_update()

  def _set_initial_update(self):
    initial_update = ProcessState(seq = self._seq,
      process = self.name(),
      run_state = ProcessRunState.FORKED,
      fork_time = self._fork_time,
      coordinator_pid = self._pid)
    self._initial_update = RunnerCkpt(process_state = initial_update)

  def has_initial_update(self):
    return self._initial_update != None

  def initial_update(self):
    update = self._initial_update
    self._initial_update = None
    return update

  def running(self):
    if self._popen:
      self._popen.poll()
    if self._popen is None or self._popen.returncode is None:
      return True
    return False

  def rc(self):
    return self._popen.returncode if not self.running() else None

  def pid(self):
    """pid of the coordinator"""
    return self._pid

  def rebind(self, pid, fork_time):
    """rebind Process to an existing coordinator pid without forking"""
    self._pid = pid
    self._fork_time = fork_time

  def cmdline(self):
    return self._cmdline

  def name(self):
    return self._name

  def ckpt_file(self):
    assert self._pid is not None
    assert self._fork_time is not None
    return (self._pathspec.given(pid = self._pid, fork_time = int(self._fork_time))
                          .getpath('process_checkpoint'))

  def die(self):
    self._log('die() called, sending SIGKILL to children.')
    if self.running():
      self._popen.kill()
    else:
      self._log('WARNING: children already dead.')
    sys.exit(1)
