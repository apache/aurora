import os

from mock import call, patch
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdir, touch

from apache.thermos.common.path import TaskPath
from apache.thermos.core.helper import TaskRunnerHelper
from apache.thermos.monitoring.garbage import TaskGarbageCollector

from gen.apache.thermos.ttypes import RunnerCkpt, RunnerHeader


def test_empty_garbage_collector():
  with temporary_dir() as checkpoint_root:
    path = TaskPath(root=checkpoint_root, task_id='does_not_exist')
    gc = TaskGarbageCollector(checkpoint_root, 'does_not_exist')

    assert gc.get_age() == 0

    # assume runner, finished task
    assert set(gc.get_metadata(with_size=False)) == set([
        path.getpath('runner_checkpoint'),
        path.given(state='finished').getpath('task_path'),
    ])

    assert list(gc.get_logs()) == []
    assert list(gc.get_data()) == []


@patch('apache.thermos.monitoring.garbage.safe_delete')
@patch('apache.thermos.monitoring.garbage.safe_rmtree')
def test_garbage_collector(safe_rmtree, safe_delete):
  with temporary_dir() as sandbox, temporary_dir() as checkpoint_root, temporary_dir() as log_dir:

    path = TaskPath(root=checkpoint_root, task_id='test', log_dir=log_dir)

    touch(os.path.join(sandbox, 'test_file1'))
    touch(os.path.join(sandbox, 'test_file2'))
    safe_mkdir(os.path.dirname(path.given(state='finished').getpath('task_path')))
    safe_mkdir(os.path.dirname(path.getpath('runner_checkpoint')))
    touch(path.given(state='finished').getpath('task_path'))

    header = RunnerHeader(task_id='test', sandbox=sandbox, log_dir=log_dir)
    ckpt = TaskRunnerHelper.open_checkpoint(path.getpath('runner_checkpoint'))
    ckpt.write(RunnerCkpt(runner_header=header))
    ckpt.close()

    gc = TaskGarbageCollector(checkpoint_root, task_id='test')
    assert gc._state.header.log_dir == log_dir
    assert gc._state.header.sandbox == sandbox

    # erase metadata
    gc.erase_metadata()
    safe_delete.assert_has_calls([
        call(path.given(state='finished').getpath('task_path')),
        call(path.getpath('runner_checkpoint'))], any_order=True)
    safe_rmtree.assert_has_calls([call(path.getpath('checkpoint_path'))])

    safe_delete.reset_mock()
    safe_rmtree.reset_mock()

    # erase logs
    gc.erase_logs()
    safe_rmtree.assert_has_calls([call(log_dir)])

    safe_delete.reset_mock()
    safe_rmtree.reset_mock()

    # erase sandbox
    gc.erase_data()

    safe_delete.assert_has_calls([
        call(os.path.join(sandbox, 'test_file1')),
        call(os.path.join(sandbox, 'test_file2'))], any_order=True)
    safe_rmtree.assert_has_calls([call(sandbox)])
