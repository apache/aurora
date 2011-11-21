import os
import sys
import tempfile
import subprocess
import time

from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner.runner import TaskRunnerHelper
from twitter.thermos.base import TaskPath, AlaCarteRunnerState

from gen.twitter.thermos.ttypes import (
  TaskState,
  TaskRunState,
  TaskRunnerCkpt,
  TaskRunnerState,
  ProcessRunState
)

class RunnerTestBase(object):
  RUN_JOB_SCRIPT = """
import os
from twitter.tcl.loader import ThermosJobLoader
from twitter.thermos.runner import TaskRunner
from twitter.thermos.runner.runner import TaskRunnerHelper

job = ThermosJobLoader('%(filename)s').to_thrift()
task = job.tasks[0]
sandbox = '%(sandbox)s'
root = '%(root)s'
task_id = '%(task_id)s'
sandbox = os.path.join(sandbox, task_id)

runner = TaskRunner(task, sandbox, root, task_id)
runner.run()
TaskRunnerHelper.dump_state(runner.state(), '%(state_filename)s')
"""

  @classmethod
  def job_specification(cls):
    raise NotImplementedError

  @classmethod
  def setup_class(cls):
    if hasattr(cls, 'initialized') and cls.initialized:
      return
    with open(tempfile.mktemp(), "w") as fp:
      cls.job_filename = fp.name
      print >> fp, cls.job_specification()
    cls.state_filename = tempfile.mktemp()
    cls.tempdir = tempfile.mkdtemp()
    cls.task_id = '%s-runner-base' % int(time.time()*1000000)
    cls.sandbox = os.path.join(cls.tempdir, 'sandbox')
    with open(tempfile.mktemp(), "w") as fp:
      cls.script_filename = fp.name
      print >> fp, cls.RUN_JOB_SCRIPT % {
        'filename': cls.job_filename,
        'sandbox': cls.sandbox,
        'root': cls.tempdir,
        'task_id': cls.task_id,
        'state_filename': cls.state_filename,
      }
    cls.pathspec = TaskPath(root = cls.tempdir, task_id = cls.task_id)
    assert subprocess.call([sys.executable, cls.script_filename]) == 0
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
    atexit.register(cleanup_handler)
