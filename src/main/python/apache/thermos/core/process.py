#
# Copyright 2013 Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Run processes of a Thermos task.

This module contains the Process class, used to manage the execution of the constituent processes of
a Thermos task. Each process is represented by a "coordinator" process, which fires off the actual
commandline in a subprocess of its own.

"""

import getpass
import grp
import os
import pwd
import signal
import subprocess
import sys
import time
from abc import abstractmethod

from twitter.common import log
from twitter.common.dirutil import lock_file, safe_mkdir, safe_open
from twitter.common.lang import Interface
from twitter.common.quantity import Amount, Time
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from gen.apache.thermos.ttypes import ProcessState, ProcessStatus, RunnerCkpt


class Platform(Interface):
  """Abstract representation of a platform encapsulating system-level functions"""
  @abstractmethod
  def clock(self):
    pass

  @abstractmethod
  def fork(self):
    pass

  @abstractmethod
  def getpid(self):
    pass


class ProcessBase(object):
  """
    Encapsulate a running process for a task.
  """
  class Error(Exception): pass
  class UnknownUserError(Error): pass
  class CheckpointError(Error): pass
  class UnspecifiedSandbox(Error): pass
  class PermissionError(Error): pass

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
      user, current_user = self._getpwuid() # may raise self.UnknownUserError
      if user != current_user and os.geteuid() != 0:
        raise self.PermissionError('Must be root to run processes as other users!')
    self._ckpt = None
    self._ckpt_head = -1
    if platform is None:
      raise ValueError("Platform must be specified")
    self._platform = platform

  def _log(self, msg):
    log.debug('[process:%5s=%s]: %s' % (self._pid, self.name(), msg))

  def _ckpt_write(self, msg):
    self._init_ckpt_if_necessary()
    self._log("child state transition [%s] <= %s" % (self.ckpt_file(), msg))
    self._ckpt.write(msg)

  def _write_process_update(self, **kw):
    """Write a process update to the coordinator's checkpoint stream."""
    process_status = ProcessStatus(**kw)
    process_status.seq = self._seq
    process_status.process = self.name()
    self._ckpt_write(RunnerCkpt(process_status=process_status))
    self._seq += 1

  def _write_initial_update(self):
    self._write_process_update(state=ProcessState.FORKED,
                               fork_time=self._fork_time,
                               coordinator_pid=self._pid)

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
    return self._pathspec.getpath('process_checkpoint')

  def _setup_ckpt(self):
    """Set up the checkpoint: must be run on the parent."""
    self._log('initializing checkpoint file: %s' % self.ckpt_file())
    ckpt_fp = lock_file(self.ckpt_file(), "a+")
    if ckpt_fp in (None, False):
      raise self.CheckpointError('Could not acquire checkpoint permission or lock for %s!' %
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
      while total_wait_time < self.MAXIMUM_CONTROL_WAIT:
        ckpt_tail = os.path.getsize(self.ckpt_file())
        if ckpt_tail == self._ckpt_head:
          self._platform.clock().sleep(self.CONTROL_WAIT_CHECK_INTERVAL.as_(Time.SECONDS))
          total_wait_time += self.CONTROL_WAIT_CHECK_INTERVAL
          continue
        checkpoint = rr.try_read()
        if checkpoint:
          if not checkpoint.process_status:
            raise self.CheckpointError('No process status in checkpoint!')
          if (checkpoint.process_status.process != self.name() or
              checkpoint.process_status.state != ProcessState.FORKED or
              checkpoint.process_status.fork_time != self._fork_time or
              checkpoint.process_status.coordinator_pid != self._pid):
            self._log('Losing control of the checkpoint stream:')
            self._log('   fork_time [%s] vs self._fork_time [%s]' % (
                checkpoint.process_status.fork_time, self._fork_time))
            self._log('   coordinator_pid [%s] vs self._pid [%s]' % (
                checkpoint.process_status.coordinator_pid, self._pid))
            raise self.CheckpointError('Lost control of the checkpoint stream!')
          self._log('Taking control of the checkpoint stream at record: %s' %
            checkpoint.process_status)
          self._seq = checkpoint.process_status.seq + 1
          return True
    raise self.CheckpointError('Timed out waiting for checkpoint stream!')

  def _prepare_fork(self):
    user, current_user = self._getpwuid()
    uid, gid = user.pw_uid, user.pw_gid
    self._fork_time = self._platform.clock().time()
    self._setup_ckpt()
    self._stdout = safe_open(self._pathspec.with_filename('stdout').getpath('process_logdir'), "w")
    self._stderr = safe_open(self._pathspec.with_filename('stderr').getpath('process_logdir'), "w")
    os.chown(self._stdout.name, user.pw_uid, user.pw_gid)
    os.chown(self._stderr.name, user.pw_uid, user.pw_gid)

  def _finalize_fork(self):
    self._write_initial_update()
    self._ckpt.close()
    self._ckpt = None

  def _getpwuid(self):
    """Returns a tuple of the user (i.e. --user) and current user."""
    try:
      current_user = pwd.getpwuid(os.getuid())
    except KeyError:
      raise self.UnknownUserError('Unknown user %s!' % self._user)
    try:
      user = pwd.getpwnam(self._user) if self._user else current_user
    except KeyError:
      raise self.UnknownUserError('Unable to get pwent information!')
    return user, current_user

  def start(self):
    """
      This is the main call point from the runner, and forks a co-ordinator process to run the
      target process (i.e. self.cmdline())

      The parent returns immediately and populates information about the pid of the co-ordinator.
      The child (co-ordinator) will launch the target process in a subprocess.
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
  RCFILE = '.thermos_profile'
  FD_CLOEXEC = True

  def __init__(self, *args, **kw):
    """
      See ProcessBase.__init__

      Takes additional arguments:
        fork: the fork function to use [default: os.fork]
        chroot: whether or not to chroot into the sandbox [default: False]
    """
    fork = kw.pop('fork', os.fork)
    self._use_chroot = bool(kw.pop('chroot', False))
    self._rc = None
    kw['platform'] = RealPlatform(fork=fork)
    ProcessBase.__init__(self, *args, **kw)
    if self._use_chroot and self._sandbox is None:
      raise self.UnspecifiedSandbox('If using chroot, must specify sandbox!')

  def _chroot(self):
    """chdir and chroot to the sandbox directory."""
    os.chdir(self._sandbox)
    os.chroot(self._sandbox)

  def _setuid(self):
    """Drop privileges to the user supplied in Process creation (if necessary.)"""
    user, current_user = self._getpwuid()
    if user.pw_uid == current_user.pw_uid:
      return

    uid, gid = user.pw_uid, user.pw_gid
    username = user.pw_name
    group_ids = [group.gr_gid for group in grp.getgrall() if username in group.gr_mem]
    os.setgroups(group_ids)
    os.setgid(gid)
    os.setuid(uid)

  def execute(self):
    """Perform final initialization and launch target process commandline in a subprocess."""
    if not self._stderr:
      raise RuntimeError('self._stderr not set up!')
    if not self._stdout:
      raise RuntimeError('self._stdout not set up!')

    user, _ = self._getpwuid()
    username, homedir = user.pw_name, user.pw_dir

    # TODO(wickman) reconsider setsid now that we're invoking in a subshell
    os.setsid()
    if self._use_chroot:
      self._chroot()
    self._setuid()

    # start process
    start_time = self._platform.clock().time()

    if not self._sandbox:
      sandbox = os.getcwd()
    else:
      sandbox = self._sandbox if not self._use_chroot else '/'

    thermos_profile = os.path.join(sandbox, self.RCFILE)
    env = {
      'HOME': homedir if self._use_chroot else sandbox,
      'LOGNAME': username,
      'USER': username,
      'PATH': os.environ['PATH']
    }

    if os.path.exists(thermos_profile):
      env.update(BASH_ENV=thermos_profile)

    self._popen = subprocess.Popen(["/bin/bash", "-c", self.cmdline()],
                                   stderr=self._stderr,
                                   stdout=self._stdout,
                                   close_fds=self.FD_CLOEXEC,
                                   cwd=sandbox,
                                   env=env)

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

    self._write_process_update(state=state,
                               return_code=rc,
                               stop_time=self._platform.clock().time())
    self._rc = rc

  def finish(self):
    self._log('Coordinator exiting.')
    sys.exit(0)
