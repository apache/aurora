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

import errno
import grp
import os
import pwd
import select
import shlex
import signal
import subprocess
import sys
import time
from abc import abstractmethod
from copy import deepcopy

from twitter.common import log
from twitter.common.dirutil import lock_file, safe_delete, safe_mkdir, safe_open
from twitter.common.lang import Interface
from twitter.common.quantity import Amount, Data, Time
from twitter.common.recordio import ThriftRecordReader, ThriftRecordWriter

from apache.thermos.common.process_util import wrap_with_mesos_containerizer

from gen.apache.aurora.api.constants import TASK_FILESYSTEM_MOUNT_POINT
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


class LoggerDestination(object):
  FILE = 'file'
  CONSOLE = 'console'
  BOTH = 'both'
  NONE = 'none'

  _ALL_DESTINATIONS = [FILE, CONSOLE, BOTH, NONE]

  @staticmethod
  def is_valid(destination):
    return destination in LoggerDestination._ALL_DESTINATIONS


class LoggerMode(object):
  STANDARD = 'standard'
  ROTATE = 'rotate'

  _ALL_MODES = [STANDARD, ROTATE]

  @staticmethod
  def is_valid(mode):
    return mode in LoggerMode._ALL_MODES


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

  def __init__(self, name, cmdline, sequence, pathspec, sandbox_dir, user=None, platform=None,
               logger_destination=LoggerDestination.FILE, logger_mode=LoggerMode.STANDARD,
               rotate_log_size=None, rotate_log_backups=None):
    """
      required:
        name        = name of the process
        cmdline     = cmdline of the process
        sequence    = the next available sequence number for state updates
        pathspec    = TaskPath object for synthesizing path names
        sandbox_dir = the sandbox in which to run the process
        platform    = Platform providing fork, clock, getpid

      optional:
        user               = the user to run as (if unspecified, will default to current user.)
                             if specified to a user that is not the current user, you must have root
                             access
        logger_destination = The destination for logs output.
        logger_mode        = The type of logger to use for the process.
        rotate_log_size    = The maximum size of the rotated stdout/stderr logs.
        rotate_log_backups = The maximum number of rotated stdout/stderr log backups.
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
    self._user = user
    self._ckpt = None
    self._ckpt_head = -1
    if platform is None:
      raise ValueError("Platform must be specified")
    self._platform = platform
    self._logger_destination = logger_destination
    self._logger_mode = logger_mode
    self._rotate_log_size = rotate_log_size
    self._rotate_log_backups = rotate_log_backups

    if not LoggerDestination.is_valid(self._logger_destination):
      raise ValueError("Logger destination %s is invalid." % self._logger_destination)

    if not LoggerMode.is_valid(self._logger_mode):
      raise ValueError("Logger mode %s is invalid." % self._logger_mode)

    if self._logger_mode == LoggerMode.ROTATE:
      if self._rotate_log_size.as_(Data.BYTES) <= 0:
        raise ValueError('Log size cannot be less than one byte.')
      if self._rotate_log_backups <= 0:
        raise ValueError('Log backups cannot be less than one.')

  def _log(self, msg):
    log.debug('[process:%5s=%s]: %s' % (self._pid, self.name(), msg))

  def _getpwuid(self):
    """Returns a tuple of the user (i.e. --user) and current user."""
    uid = os.getuid()
    try:
      current_user = pwd.getpwuid(uid)
    except KeyError:
      raise self.UnknownUserError('Unknown uid %s!' % uid)
    try:
      user = pwd.getpwnam(self._user) if self._user is not None else current_user
    except KeyError:
      raise self.UnknownUserError('Unable to get pwent information!')
    return user, current_user

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

  def process_logdir(self):
    return self._pathspec.getpath('process_logdir')

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
    if self._user:
      if user != current_user and os.geteuid() != 0:
        raise self.PermissionError('Must be root to run processes as other users!')
    self._fork_time = self._platform.clock().time()
    self._setup_ckpt()
    # Since the forked process is responsible for creating log files, it needs to own the log dir.
    safe_mkdir(self.process_logdir())
    os.chown(self.process_logdir(), user.pw_uid, user.pw_gid)

  def _finalize_fork(self):
    self._write_initial_update()
    self._ckpt.close()
    self._ckpt = None

  def start(self):
    """
      This is the main call point from the runner, and forks a co-ordinator process to run the
      target process (i.e. self.cmdline())

      The parent returns immediately and populates information about the pid of the co-ordinator.
      The child (co-ordinator) will launch the target process in a subprocess.
    """
    self._prepare_fork()  # calls _setup_ckpt which can raise CheckpointError
                          # calls _getpwuid which can raise:
                          #    UnknownUserError
                          #    PermissionError
    self._pid = self._platform.fork()
    if self._pid == 0:
      self._pid = self._platform.getpid()
      self._wait_for_control()  # can raise CheckpointError
      try:
        self.execute()
      except Exception as e:
        self._log('Error trying to execute %s: %s' % (self._name, e))
        raise e
      finally:
        self._ckpt.close()
        self.finish()
    else:
      self._finalize_fork()  # can raise CheckpointError

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
        preserve_env: whether or not to preserve env variables for the task [default: False]
        mesos_containerizer_path: The path to the mesos-containerizer binary to be used for task
                                  filesystem isolation.
        container_sandbox: If running in an isolated filesystem, the path within that filesystem
                           where the sandbox is mounted.
    """
    fork = kw.pop('fork', os.fork)
    self._use_chroot = bool(kw.pop('chroot', False))
    self._rc = None
    self._preserve_env = bool(kw.pop('preserve_env', False))
    self._mesos_containerizer_path = kw.pop('mesos_containerizer_path', None)
    self._container_sandbox = kw.pop('container_sandbox', None)

    if self._mesos_containerizer_path is not None and self._container_sandbox is None:
      raise self.UnspecifiedSandbox('If using mesos-containerizer, container_sandbox must be set.')

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

  def wrapped_cmdline(self, cwd):
    cmdline = self.cmdline()

    # If mesos-containerizer is not set, we only need to wrap the cmdline in a bash invocation.
    if self._mesos_containerizer_path is None:
      return ['/bin/bash', '-c', cmdline]

    return shlex.split(
        wrap_with_mesos_containerizer(cmdline, self._user, cwd, self._mesos_containerizer_path))

  def execute(self):
    """Perform final initialization and launch target process commandline in a subprocess."""

    user, _ = self._getpwuid()
    username, homedir = user.pw_name, user.pw_dir

    # TODO(wickman) reconsider setsid now that we're invoking in a subshell
    os.setsid()
    if self._use_chroot:
      self._chroot()

    # If the mesos containerizer path is set, then this process will be launched from within an
    # isolated filesystem image by the mesos-containerizer executable. This executable needs to be
    # run as root so that it can properly set up the filesystem as such we'll skip calling setuid at
    # this point. We'll instead setuid after the process has been forked (mesos-containerizer itself
    # ensures the forked process is run as the correct user).
    taskfs_isolated = self._mesos_containerizer_path is not None
    if not taskfs_isolated:
      self._setuid()

    # start process
    start_time = self._platform.clock().time()

    if not self._sandbox:
      cwd = subprocess_cwd = sandbox = os.getcwd()
    else:
      if self._use_chroot:
        cwd = subprocess_cwd = sandbox = '/'
      elif taskfs_isolated:
        cwd = homedir = sandbox = self._container_sandbox
        subprocess_cwd = self._sandbox
      else:
        cwd = subprocess_cwd = homedir = sandbox = self._sandbox

    thermos_profile = os.path.join(sandbox, self.RCFILE)

    if self._preserve_env:
      env = deepcopy(os.environ)
    else:
      env = {}

    env.update({
      'HOME': homedir,
      'LOGNAME': username,
      'USER': username,
      'PATH': os.environ['PATH']
    })

    wrapped_cmdline = self.wrapped_cmdline(cwd)
    log.debug('Wrapped cmdline: %s' % wrapped_cmdline)

    real_thermos_profile_path = os.path.join(
        os.environ['MESOS_DIRECTORY'],
        TASK_FILESYSTEM_MOUNT_POINT,
        thermos_profile.lstrip('/')) if taskfs_isolated else thermos_profile

    if os.path.exists(real_thermos_profile_path):
      env.update(BASH_ENV=thermos_profile)

    log.debug('ENV is: %s' % env)
    subprocess_args = {
      'args': wrapped_cmdline,
      'close_fds': self.FD_CLOEXEC,
      'cwd': subprocess_cwd,
      'env': env,
      'pathspec': self._pathspec
    }

    log_destination_resolver = LogDestinationResolver(
        self._pathspec,
        destination=self._logger_destination,
        mode=self._logger_mode,
        rotate_log_size=self._rotate_log_size,
        rotate_log_backups=self._rotate_log_backups)
    stdout, stderr, handlers_are_files = log_destination_resolver.get_handlers()
    if handlers_are_files:
      executor = SubprocessExecutor(stdout=stdout, stderr=stderr, **subprocess_args)
    else:
      executor = PipedSubprocessExecutor(stdout=stdout, stderr=stderr, **subprocess_args)

    pid = executor.start()

    # Now that we've forked the process, if the task's filesystem is isolated it's now safe to
    # setuid.
    if taskfs_isolated:
      self._setuid()

    self._write_process_update(state=ProcessState.RUNNING, pid=pid, start_time=start_time)

    rc = executor.wait()

    # indicate that we have finished/failed
    if rc < 0:
      state = ProcessState.KILLED
    elif rc == 0:
      state = ProcessState.SUCCESS
    else:
      state = ProcessState.FAILED

    self._write_process_update(state=state, return_code=rc, stop_time=self._platform.clock().time())
    self._rc = rc

  def finish(self):
    self._log('Coordinator exiting.')
    sys.exit(0)


class SubprocessExecutorBase(object):
  """
  Encapsulate execution of a subprocess.
  """

  def __init__(self, args, close_fds, cwd, env, pathspec):
    """
      required:
        args        = The arguments to pass to the subprocess.
        close_fds   = Close file descriptors argument to Popen.
        cwd         = The current working directory.
        env         = Environment variables to be passed to the subprocess.
        pathspec    = TaskPath object for synthesizing path names.
    """
    self._args = args
    self._close_fds = close_fds
    self._cwd = cwd
    self._env = env
    self._pathspec = pathspec
    self._popen = None

  def _start_subprocess(self, stderr, stdout):
    return subprocess.Popen(self._args,
                            stderr=stderr,
                            stdout=stdout,
                            close_fds=self._close_fds,
                            cwd=self._cwd,
                            env=self._env)

  def start(self):
    """Start the subprocess and immediately return the resulting pid."""
    raise NotImplementedError()

  def wait(self):
    """Wait for the subprocess to finish executing and return the return code."""
    raise NotImplementedError()


class SubprocessExecutor(SubprocessExecutorBase):
  """
  Basic implementation of a SubprocessExecutor that writes stderr/stdout to specified output files.
  """

  def __init__(self, args, close_fds, cwd, env, pathspec, stdout=None, stderr=None):
    """
    See SubprocessExecutorBase.__init__

    Takes additional arguments:
      stdout = Destination handler for stdout output. Default is /dev/null.
      stderr = Destination handler for stderr output. Default is /dev/null.
    """
    super(SubprocessExecutor, self).__init__(args, close_fds, cwd, env, pathspec)
    self._stderr = stderr
    self._stdout = stdout

  def start(self):
    self._popen = self._start_subprocess(self._stderr, self._stdout)
    return self._popen.pid

  def wait(self):
    return self._popen.wait()


class PipedSubprocessExecutor(SubprocessExecutorBase):
  """
  Implementation of SubprocessExecutorBase that uses pipes to poll the pipes to output streams and
  copies them to the specified destinations.
  """

  READ_BUFFER_SIZE = 2 ** 16

  def __init__(self, args, close_fds, cwd, env, pathspec, stdout=None, stderr=None):
    """
    See SubprocessExecutorBase.__init__

    Takes additional arguments:
      stdout = Destination handler for stdout output. Default is /dev/null.
      stderr = Destination handler for stderr output. Default is /dev/null.
    """
    super(PipedSubprocessExecutor, self).__init__(args, close_fds, cwd, env, pathspec)
    self._stderr = stderr
    self._stdout = stdout

  def start(self):
    self._popen = self._start_subprocess(subprocess.PIPE, subprocess.PIPE)
    return self._popen.pid

  def wait(self):
    stdout = self._popen.stdout.fileno()
    stderr = self._popen.stderr.fileno()
    pipes = {
      stderr: self._stderr,
      stdout: self._stdout
    }

    rc = None
    # Read until there is a return code AND both of the pipes have reached EOF.
    while rc is None or pipes:
      rc = self._popen.poll()

      read_results, _, _ = select.select(pipes.keys(), [], [], 1)
      for fd in read_results:
        handler = pipes[fd]
        buf = os.read(fd, self.READ_BUFFER_SIZE)

        if len(buf) == 0:
          del pipes[fd]
        else:
          handler.write(buf)

    return rc


class LogDestinationResolver(object):
  """
  Resolves correct stdout/stderr destinations based on process configuration.
  """

  STDOUT = 'stdout'
  STDERR = 'stderr'

  def __init__(self, pathspec, destination=LoggerDestination.FILE, mode=LoggerMode.STANDARD,
               rotate_log_size=None, rotate_log_backups=None):
    """
    pathspec           = TaskPath object for synthesizing path names.
    destination        = Log destination.
    logger_mode        = The type of logger to use for the process.
    rotate_log_size    = The maximum size of the rotated stdout/stderr logs.
    rotate_log_backups = The maximum number of rotated stdout/stderr log backups.
    """
    self._pathspec = pathspec
    self._destination = destination
    self._mode = mode
    self._rotate_log_size = rotate_log_size
    self._rotate_log_backups = rotate_log_backups

    if not LoggerDestination.is_valid(self._destination):
      raise ValueError("Logger destination %s is invalid." % self._destination)

    if not LoggerMode.is_valid(self._mode):
      raise ValueError("Logger mode %s is invalid." % self._mode)

  def get_handlers(self):
    """
    Creates stdout/stderr handler by provided configuration
    """
    return (self._get_handler(self.STDOUT),
            self._get_handler(self.STDERR),
            self._handlers_are_files())

  def _handlers_are_files(self):
    """
    Returns True if both the handlers are standard file objects.
    """
    return (self._destination == LoggerDestination.CONSOLE or
        (self._destination == LoggerDestination.FILE and self._mode == LoggerMode.STANDARD))

  def _get_handler(self, name):
    """
    Constructs correct handler or file object based on the provided configuration.
    """

    # On no destination write logs to /dev/null
    if self._destination == LoggerDestination.NONE:
      return StreamHandler(safe_open(os.devnull, 'w'))

    # Streamed logs to predefined outputs
    if self._destination == LoggerDestination.CONSOLE:
      return sys.stdout if name == self.STDOUT else sys.stderr

    # Streaming AND file logs are required
    if self._destination == LoggerDestination.BOTH:
      return TeeHandler(self._get_stream(name), self._get_file(name))

    # File only logs are required
    return self._get_file(name)

  def _get_file(self, name):
    if self._mode == LoggerMode.STANDARD:
      return safe_open(self._get_log_path(name), mode='a')
    if self._mode == LoggerMode.ROTATE:
      log_size = int(self._rotate_log_size.as_(Data.BYTES))
      return RotatingFileHandler(self._get_log_path(name),
                                 log_size,
                                 self._rotate_log_backups)

  def _get_stream(self, name):
    """
    Returns OS stream by name
    """
    if name == self.STDOUT:
      return StreamHandler(sys.stdout)
    if name == self.STDERR:
      return StreamHandler(sys.stderr)

  def _get_log_path(self, log_name):
    return self._pathspec.with_filename(log_name).getpath('process_logdir')


class RotatingFileHandler(object):
  """
  File handler that implements max size/rotation.
  """

  def __init__(self, filename, max_bytes, max_backups, mode='w'):
    """
      required:
        filename    = The file name.
        max_bytes   = The maximum size of an individual log file.
        max_backups = The maximum number of log file backups to create.

      optional:
        mode = Mode to open the file in.
    """
    if max_bytes > 0 and max_backups <= 0:
      raise ValueError('A positive value for max_backups must be specified if max_bytes > 0.')
    self._max_bytes = max_bytes
    self._max_backups = max_backups
    self.file = safe_open(filename, mode=mode)
    self.filename = filename
    self.mode = mode
    self.closed = False

  def close(self):
    if not self.closed:
      self.file.close()
      self.closed = True

  def write(self, b):
    self.file.write(b)
    self.file.flush()
    if self.should_rollover():
      self.rollover()

  def swap_files(self, src, tgt):
    if os.path.exists(tgt):
      safe_delete(tgt)

    try:
      os.rename(src, tgt)
    except OSError as e:
      if e.errno != errno.ENOENT:
        raise

  def make_indexed_filename(self, index):
    return '%s.%d' % (self.filename, index)

  def should_rollover(self):
    if self._max_bytes <= 0 or self._max_backups <= 0:
      return False

    if self.file.tell() >= self._max_bytes:
      return True

    return False

  def rollover(self):
    """
    Perform the rollover of the log.
    """
    self.file.close()
    for i in range(self._max_backups - 1, 0, -1):
      src = self.make_indexed_filename(i)
      tgt = self.make_indexed_filename(i + 1)
      if os.path.exists(src):
        self.swap_files(src, tgt)

    self.swap_files(self.filename, self.make_indexed_filename(1))
    self.file = safe_open(self.filename, mode='w')


class StreamHandler(object):
  """
  Stream handler wraps stream objects and allows configuration of whether objects
  should be closed when ending a subprocess.
  """

  def __init__(self, stream, close=False):
    """
    stream = Wrapped stream object.
    """
    self._stream = stream
    self._close = close

  def write(self, b):
    self._stream.write(b)
    self._stream.flush()

  def close(self):
    if self._close:
      self._stream.close()


class TeeHandler(object):
  """
  Tee handler mimicks the unix tee command and splits output between two destinations
  """

  def __init__(self, first, second):
    """
      required:
        first  = First destination
        second = Second destination
    """
    self._first = first
    self._second = second

  def write(self, b):
    self._first.write(b)
    self._second.write(b)

  def close(self):
    self._first.close()
    self._second.close()
