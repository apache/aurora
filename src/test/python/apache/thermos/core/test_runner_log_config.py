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

import os
import struct
from collections import namedtuple

from twitter.common.quantity import Amount, Data

from apache.thermos.config.schema_base import Logger, Process, RotatePolicy, Task
from apache.thermos.testing.runner import RunnerTestBase


class LogConfig(namedtuple('LogConfig', ['destination', 'mode', 'size_mb', 'backups'])):
  @classmethod
  def create(cls, destination=None, mode=None, size_mb=None, backups=None):
    return cls(destination=destination, mode=mode, size_mb=size_mb, backups=backups)


class RunnerLogConfigTestBase(RunnerTestBase):
  _OPT_CONFIG_PROCESS_NAME = 'opt-log-config-process'
  _CUSTOM_CONFIG_PROCESS_NAME = 'custom-log-config-process'

  @classmethod
  def opt_config(cls):
    return LogConfig.create()

  @classmethod
  def custom_config(cls):
    return None

  @classmethod
  def log_size(cls):
    return Amount(1, Data.BYTES)

  STDOUT = 1
  STDERR = 2

  @classmethod
  def log_fd(cls):
    return cls.STDOUT

  @classmethod
  def extra_task_runner_args(cls):
    opt_config = cls.opt_config()
    return dict(process_logger_destination=opt_config.destination,
                process_logger_mode=opt_config.mode,
                rotate_log_size_mb=opt_config.size_mb,
                rotate_log_backups=opt_config.backups)

  @classmethod
  def task(cls):
    cmdline = 'head -c %d /dev/zero >&%d' % (cls.log_size().as_(Data.BYTES), cls.log_fd())

    opt_config_process = Process(name=cls._OPT_CONFIG_PROCESS_NAME, cmdline=cmdline)

    custom_config_process = Process(name=cls._CUSTOM_CONFIG_PROCESS_NAME, cmdline=cmdline)
    custom_config = cls.custom_config()
    if custom_config:
      logger = Logger()
      if custom_config.destination:
        logger = logger(destination=custom_config.destination)
      if custom_config.mode:
        logger = logger(mode=custom_config.mode)
      if custom_config.size_mb or custom_config.backups:
        rotate = RotatePolicy()
        if custom_config.size_mb:
          rotate = rotate(log_size=custom_config.size_mb)
        if custom_config.backups:
          rotate = rotate(backups=custom_config.backups)
        logger = logger(rotate=rotate)
      custom_config_process = custom_config_process(logger=logger)

    return Task(name='log-config-task', processes=[opt_config_process, custom_config_process])

  class Assert(object):
    def __init__(self, log_file_dir):
      self._log_file_dir = log_file_dir

    def log_file_names(self, *names):
      actual_names = os.listdir(self._log_file_dir)
      assert set(actual_names) == set(names)
      assert all(os.path.isfile(os.path.join(self._log_file_dir, name)) for name in names)

    def log_file(self, name):
      expected_output_file = os.path.join(self._log_file_dir, name)
      assert os.path.exists(expected_output_file)
      with open(expected_output_file, 'rb') as fp:
        actual = fp.read()
        size = len(actual)
        assert actual == struct.pack('B', 0) * size
      return size

    def empty_log_file(self, name):
      size = self.log_file(name=name)
      assert 0 == size

  def _log_dir_name(self, process_name):
    return os.path.join(self.state.header.log_dir, process_name, '0')

  @property
  def opt_assert(self):
    return self.Assert(self._log_dir_name(self._OPT_CONFIG_PROCESS_NAME))

  @property
  def custom_assert(self):
    return self.Assert(self._log_dir_name(self._CUSTOM_CONFIG_PROCESS_NAME))


class StandardTestBase(RunnerLogConfigTestBase):
  @classmethod
  def log_size(cls):
    return Amount(200, Data.MB)

  def test_log_config(self):
    log, empty = ('stdout', 'stderr') if self.log_fd() == self.STDOUT else ('stderr', 'stdout')
    for assertions in self.opt_assert, self.custom_assert:
      assertions.log_file_names(log, empty)
      assertions.empty_log_file(name=empty)

      # No rotation should occur in standard mode.
      size = assertions.log_file(name=log)
      assert size == self.log_size().as_(Data.BYTES)


class TestStandardStdout(StandardTestBase):
  @classmethod
  def log_fd(cls):
    return cls.STDOUT


class TestStandardStderr(StandardTestBase):
  @classmethod
  def log_fd(cls):
    return cls.STDERR


class RotateTestBase(RunnerLogConfigTestBase):
  @classmethod
  def opt_config(cls):
    return LogConfig.create(mode='rotate', size_mb=1, backups=1)


class TestRotateUnderStdout(RotateTestBase):
  def test_log_config(self):
    self.opt_assert.log_file_names('stdout', 'stderr')
    self.opt_assert.empty_log_file(name='stderr')
    self.opt_assert.log_file(name='stdout')


class TestRotateUnderStderr(RotateTestBase):
  @classmethod
  def log_fd(cls):
    return cls.STDERR

  def test_log_config(self):
    self.opt_assert.log_file_names('stdout', 'stderr')
    self.opt_assert.empty_log_file(name='stdout')
    self.opt_assert.log_file(name='stderr')


class TestRotateOverStdout(RotateTestBase):
  @classmethod
  def log_size(cls):
    return Amount(2, Data.MB)

  def test_log_config(self):
    self.opt_assert.log_file_names('stdout', 'stdout.1', 'stderr')
    self.opt_assert.empty_log_file(name='stderr')
    self.opt_assert.log_file(name='stdout')
    self.opt_assert.log_file(name='stdout.1')


class TestRotateOverStderr(RotateTestBase):
  @classmethod
  def log_size(cls):
    return Amount(3, Data.MB)

  @classmethod
  def log_fd(cls):
    return cls.STDERR

  def test_log_config(self):
    self.opt_assert.log_file_names('stdout', 'stderr', 'stderr.1')
    self.opt_assert.empty_log_file(name='stdout')
    self.opt_assert.log_file(name='stderr')
    self.opt_assert.log_file(name='stderr.1')


class TestRotateDefaulted(RunnerLogConfigTestBase):
  @classmethod
  def opt_config(cls):
    return LogConfig.create(mode='rotate')

  @classmethod
  def custom_config(cls):
    return LogConfig.create(mode='rotate')

  @classmethod
  def log_size(cls):
    # Default rotation policy is 100MiB with 5 backups so this guarantees a full rotation.
    return Amount(700, Data.MB)

  def test_log_config(self):
    for assertions in self.opt_assert, self.custom_assert:
      assertions.log_file_names('stderr',
                                'stdout',
                                'stdout.1',
                                'stdout.2',
                                'stdout.3',
                                'stdout.4',
                                'stdout.5')
      assertions.empty_log_file(name='stderr')
      assertions.log_file(name='stdout')
      assertions.log_file(name='stdout.1')
      assertions.log_file(name='stdout.2')
      assertions.log_file(name='stdout.3')
      assertions.log_file(name='stdout.4')
      assertions.log_file(name='stdout.5')
