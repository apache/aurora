import os
import threading
import time

from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdir
from twitter.common.recordio import ThriftRecordReader

from twitter.thermos.base.path import TaskPath
from twitter.thermos.runner.process import Process

from gen.twitter.thermos.ttypes import RunnerCkpt


class TestProcess(Process):
  def execute(self):
    super(TestProcess, self).execute()
    os._exit(0)
  def finish(self):
    pass


def wait_for_rc(checkpoint, timeout=5.0):
  start = time.time()
  trr = ThriftRecordReader(open(checkpoint), RunnerCkpt)
  while time.time() < start + timeout:
    record = trr.read()
    if record and record.process_status and record.process_status.return_code is not None:
      return record.process_status.return_code
    else:
      time.sleep(0.1)


def test_simple_process():
  with temporary_dir() as td:
    taskpath = TaskPath(root=td, task_id='task', process='process', run=0)
    sandbox = os.path.join(td, 'sandbox')
    safe_mkdir(sandbox)
    safe_mkdir(os.path.dirname(taskpath.getpath('process_checkpoint')))

    p = TestProcess('process', 'echo hello world', 0, taskpath, sandbox)
    p.start()
    rc = wait_for_rc(taskpath.getpath('process_checkpoint'))

    assert rc == 0
    assert os.path.exists(taskpath.with_filename('stdout').getpath('process_logdir'))
    with open(taskpath.with_filename('stdout').getpath('process_logdir'), 'r') as fp:
      assert fp.read() == 'hello world\n'


def test_cloexec():
  def run_with_class(process_class):
    with temporary_dir() as td:
      taskpath = TaskPath(root=td, task_id='task', process='process', run=0)
      sandbox = os.path.join(td, 'sandbox')
      safe_mkdir(sandbox)
      safe_mkdir(os.path.dirname(taskpath.getpath('process_checkpoint')))
      with open(os.path.join(sandbox, 'silly_pants'), 'w') as silly_pants:
        p = process_class('process', 'echo test >&%s' % silly_pants.fileno(),
            0, taskpath, sandbox)
        p.start()
        return wait_for_rc(taskpath.getpath('process_checkpoint'))

  class TestWithoutCloexec(TestProcess):
    FD_CLOEXEC = False

  assert run_with_class(TestWithoutCloexec) == 0
  assert run_with_class(TestProcess) != 0
