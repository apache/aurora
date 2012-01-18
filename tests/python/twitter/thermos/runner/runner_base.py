import os
import sys
import tempfile
import subprocess
import time

from twitter.common import log
from twitter.common.contextutil import temporary_file
from twitter.thermos.base import TaskPath, AlaCarteRunnerState
from twitter.thermos.config.loader import ThermosTaskWrapper
from twitter.thermos.runner.runner import TaskRunnerHelper

from gen.twitter.thermos.ttypes import (
  TaskState,
  TaskRunState,
  TaskRunnerCkpt,
  TaskRunnerState,
  ProcessRunState
)

import pytest

# TODO(wickman) setup_class / teardown_class don't behave like you'd want here.

@pytest.mark.xfail(reason = 'Jenkins needs to be upgraded to Python 2.6.7')
class RunnerTestBase(object):
  RUN_JOB_SCRIPT = """
import os
from twitter.common import log
from twitter.common.log.options import LogOptions
from twitter.thermos.config.loader import ThermosConfigLoader
from twitter.thermos.runner import TaskRunner
from twitter.thermos.runner.runner import TaskRunnerHelper

log.init('runner_base')
LogOptions.set_disk_log_level('DEBUG')

task = ThermosConfigLoader.load_json('%(filename)s')
task = task.tasks()[0].task

sandbox = os.path.join('%(sandbox)s', '%(task_id)s')
args = {}
args['task_id'] = '%(task_id)s'
if %(portmap)s:
  args['portmap'] = %(portmap)s

runner = TaskRunner(task, '%(root)s', sandbox, **args)
runner.run()

TaskRunnerHelper.dump_state(runner.state(), '%(state_filename)s')
"""

  @classmethod
  def task(cls):
    raise NotImplementedError

  @classmethod
  def setup_class(cls):
    if hasattr(cls, 'initialized') and cls.initialized:
      return

    with temporary_file(cleanup=False) as fp:
      cls.job_filename = fp.name
      fp.write(ThermosTaskWrapper(cls.task()).to_json())

    cls.state_filename = tempfile.mktemp()
    cls.tempdir = tempfile.mkdtemp()
    cls.task_id = '%s-runner-base' % int(time.time()*1000000)
    cls.sandbox = os.path.join(cls.tempdir, 'sandbox')

    with temporary_file(cleanup=False) as fp:
      cls.script_filename = fp.name
      fp.write(cls.RUN_JOB_SCRIPT % {
        'filename': cls.job_filename,
        'sandbox': cls.sandbox,
        'root': cls.tempdir,
        'task_id': cls.task_id,
        'state_filename': cls.state_filename,
        'portmap': repr({} if not hasattr(cls, 'portmap') else cls.portmap)
      })

    cls.pathspec = TaskPath(root = cls.tempdir, task_id = cls.task_id)
    po = subprocess.Popen([sys.executable, cls.script_filename],
      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    so, se = po.communicate()
    assert po.returncode == 0,\
    """
      Runner failed!

      config:%s\n\n\n
      stdout:%s\n\n\n
      stderr:%s\n\n\n
    """ % (open(cls.job_filename).read() if os.path.exists(cls.job_filename) else 'Nonexistent!',
           so, se)

    try:
      cls.state = TaskRunnerHelper.read_state(cls.state_filename)
    except:
      cls.state = TaskRunnerState()
    try:
      cls.reconstructed_state = AlaCarteRunnerState(cls.pathspec.getpath('runner_checkpoint')).state()
    except:
      cls.reconstructed_state = None
    cls.initialized = True

  @classmethod
  def teardown_class(cls):
    if hasattr(cls, 'exit_handler') and cls.exit_handler is not Null:
      return
    import atexit
    import shutil
    def cleanup_handler():
      os.unlink(cls.job_filename)
      os.unlink(cls.script_filename)
      shutil.rmtree(cls.tempdir, ignore_errors=True)
    cls.exit_handler = cleanup_handler
    atexit.register(cleanup_handler)
