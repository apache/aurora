from __future__ import print_function

import atexit
import errno
import os
import shutil
import subprocess
import sys
import tempfile
import time

from twitter.common import log
from twitter.common.contextutil import temporary_file, environment_as
from twitter.thermos.base.path import TaskPath
from twitter.thermos.base.ckpt import CheckpointDispatcher
from twitter.thermos.config.loader import ThermosTaskWrapper
from thrift.TSerialization import deserialize as thrift_deserialize

from gen.twitter.thermos.ttypes import (
  TaskState,
  RunnerCkpt,
  RunnerState,
)


class Runner(object):
  RUN_JOB_SCRIPT = """
import os
import random
import sys
from twitter.common import log
from twitter.common.log.options import LogOptions
from twitter.thermos.config.loader import ThermosConfigLoader
from twitter.thermos.runner import TaskRunner
from twitter.thermos.runner.helper import TaskRunnerHelper
from twitter.thermos.runner.runner import TaskRunnerUniversalHandler
from thrift.TSerialization import serialize as thrift_serialize

random.seed(%(random_seed)d)

log.init('runner_base')
LogOptions.set_disk_log_level('DEBUG')

task = ThermosConfigLoader.load_json('%(filename)s')
task = task.tasks()[0].task

success_rate=%(success_rate)d

class AngryHandler(TaskRunnerUniversalHandler):
  def checkpoint(self, record):
    if not self._runner._recovery:
      if random.randint(0, 100) <= success_rate:
        super(AngryHandler, self).checkpoint(record)
      else:
        sys.exit(1)

sandbox = os.path.join('%(sandbox)s', '%(task_id)s')
args = {}
args['task_id'] = '%(task_id)s'
if %(portmap)s:
  args['portmap'] = %(portmap)s
args['universal_handler'] = AngryHandler

runner = TaskRunner(task, '%(root)s', sandbox, **args)
runner.run()

with open('%(state_filename)s', 'w') as fp:
  fp.write(thrift_serialize(runner.state))
"""

  def __init__(self, task, portmap={}, success_rate=100, random_seed=31337):
    """
      task = Thermos task
      portmap = port map
      success_rate = success rate of writing checkpoint to disk
    """
    self.task = task

    with temporary_file(cleanup=False) as fp:
      self.job_filename = fp.name
      fp.write(ThermosTaskWrapper(task).to_json())

    self.state_filename = tempfile.mktemp()
    self.tempdir = tempfile.mkdtemp()
    self.task_id = '%s-runner-base' % int(time.time()*1000000)
    self.sandbox = os.path.join(self.tempdir, 'sandbox')
    self.portmap = portmap
    self.cleaned = False
    self.pathspec = TaskPath(root = self.tempdir, task_id = self.task_id)
    self.script_filename = None
    self.success_rate = success_rate
    self.random_seed = random_seed
    self._run_count = 0

  @property
  def pid(self):
    return self.po.pid

  @property
  def root(self):
    return self.tempdir

  def run(self):
    self._run_count += 1
    atexit.register(self.cleanup)

    if self.script_filename:
      os.unlink(self.script_filename)

    with temporary_file(cleanup=False) as fp:
      self.script_filename = fp.name
      fp.write(self.RUN_JOB_SCRIPT % {
        'filename': self.job_filename,
        'sandbox': self.sandbox,
        'root': self.tempdir,
        'task_id': self.task_id,
        'state_filename': self.state_filename,
        'portmap': repr(self.portmap),
        'success_rate': self.success_rate,
        'random_seed': self.random_seed + self._run_count,
      })

    with environment_as(PYTHONPATH=os.pathsep.join(sys.path)):
      self.po = subprocess.Popen([sys.executable, self.script_filename],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      try:
        so, se = self.po.communicate()
      except OSError as e:
        if e.errno == errno.ECHILD:
          so = se = 'Killed'
        else:
          raise

    rc = self.po.returncode
    if rc != 0:
      if os.path.exists(self.job_filename):
        config = open(self.job_filename).read()
      else:
        config = 'Nonexistent!'
      if 'THERMOS_DEBUG' in os.environ:
        print("Runner failed!\n\n\nconfig:%s\n\n\nstdout:%s\n\n\nstderr:%s\n\n\n" % (
            config, so, se))

    try:
      with open(self.state_filename, 'r') as fp:
        self.state = thrift_deserialize(RunnerState(), fp.read())
    except Exception as e:
      if 'THERMOS_DEBUG' in os.environ:
        print('Failed to load Runner state: %s' % e, file=sys.stderr)
      self.state = RunnerState()

    try:
      self.reconstructed_state = CheckpointDispatcher.from_file(
        self.pathspec.getpath('runner_checkpoint'))
    except:
      self.reconstructed_state = None
    self.initialized = True
    return rc

  def cleanup(self):
    if not self.cleaned:
      if hasattr(self, 'po'):
        try:
          self.po.kill()
        except:
          pass
      os.unlink(self.job_filename)
      os.unlink(self.script_filename)
      if 'THERMOS_DEBUG' not in os.environ:
        shutil.rmtree(self.tempdir, ignore_errors=True)
      else:
        print('Logs saved in %s' % self.tempdir)
      self.cleaned = True


class RunnerTestBase(object):
  @classmethod
  def task(cls):
    raise NotImplementedError

  @classmethod
  def setup_class(cls):
    cls.runner = Runner(cls.task(), portmap=getattr(cls, 'portmap', {}))
    cls.runner.run()
    cls.state = cls.runner.state

  @classmethod
  def teardown_class(cls):
    cls.runner.cleanup()

  def test_runner_state_reconstruction(self):
    assert self.state == self.runner.reconstructed_state
