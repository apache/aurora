import os

import pytest
from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import touch

from apache.thermos.common.constants import DEFAULT_CHECKPOINT_ROOT
from apache.thermos.common.path import TaskPath
from apache.thermos.monitoring.detector import ChainedPathDetector, FixedPathDetector, TaskDetector


def test_task_detector():
  with temporary_dir() as root:
    active_log_dir = os.path.join(root, 'active_log')
    finished_log_dir = os.path.join(root, 'finished_log')

    path = TaskPath(root=root)
    detector = TaskDetector(root)

    # test empty paths

    assert list(detector.get_task_ids(state='active')) == []
    assert list(detector.get_task_ids(state='finished')) == []
    assert set(detector.get_task_ids()) == set()

    assert detector.get_checkpoint(task_id='active_task') == path.given(
        task_id='active_task').getpath('runner_checkpoint')

    assert detector.get_checkpoint(task_id='finished_task') == path.given(
        task_id='finished_task').getpath('runner_checkpoint')

    assert set(detector.get_process_checkpoints('active_task')) == set()
    assert set(detector.get_process_checkpoints('finished_task')) == set()
    assert set(detector.get_process_runs('active_task', active_log_dir)) == set()
    assert set(detector.get_process_runs('finished_task', finished_log_dir)) == set()
    assert set(detector.get_process_logs('active_task', active_log_dir)) == set()
    assert set(detector.get_process_logs('finished_task', finished_log_dir)) == set()

    # create paths

    paths = [
        path.given(state='active', task_id='active_task').getpath('task_path'),
        path.given(state='finished', task_id='finished_task').getpath('task_path'),
        path.given(task_id='active_task').getpath('runner_checkpoint'),
        path.given(task_id='finished_task').getpath('runner_checkpoint'),
        path.given(
            task_id='active_task',
            process='hello_world',
            run='0',
            log_dir=active_log_dir
        ).with_filename('stdout').getpath('process_logdir'),
        path.given(
            task_id='finished_task',
            process='goodbye_world',
            run='1',
            log_dir=finished_log_dir
        ).with_filename('stderr').getpath('process_logdir'),
        path.given(task_id='active_task', process='hello_world').getpath('process_checkpoint'),
        path.given(task_id='finished_task', process='goodbye_world').getpath('process_checkpoint'),
    ]

    for p in paths:
      touch(p)

    detector = TaskDetector(root)

    assert list(detector.get_task_ids(state='active')) == list([('active', 'active_task')])
    assert list(detector.get_task_ids(state='finished')) == list([('finished', 'finished_task')])
    assert set(detector.get_task_ids()) == set(
        [('active', 'active_task'), ('finished', 'finished_task')])

    assert list(detector.get_process_checkpoints('active_task')) == [
        path.given(task_id='active_task', process='hello_world').getpath('process_checkpoint')]

    assert list(detector.get_process_checkpoints('finished_task')) == [
        path.given(task_id='finished_task', process='goodbye_world').getpath('process_checkpoint')]

    assert list(detector.get_process_runs('active_task', active_log_dir)) == [
        ('hello_world', 0)]
    assert list(detector.get_process_runs('finished_task', finished_log_dir)) == [
        ('goodbye_world', 1)]

    assert list(detector.get_process_logs('active_task', active_log_dir)) == [
        path.given(
            task_id='active_task',
            process='hello_world',
            run='0',
            log_dir=active_log_dir
        ).with_filename('stdout').getpath('process_logdir')]

    assert list(detector.get_process_logs('finished_task', finished_log_dir)) == [
        path.given(
            task_id='finished_task',
            process='goodbye_world',
            run='1',
            log_dir=finished_log_dir
        ).with_filename('stderr').getpath('process_logdir')]


def test_fixed_path_detector():
  # Default is TaskPath default
  fpd = FixedPathDetector()
  assert fpd.get_paths() == [DEFAULT_CHECKPOINT_ROOT]

  # Non-default
  root = '/var/lib/derp'
  fpd = FixedPathDetector(path=root)
  assert fpd.get_paths() == [root]


def test_fixed_path_detector_constructor():
  with pytest.raises(TypeError):
    FixedPathDetector(path=234)


def test_chained_path_detector():
  root1 = '/var/lib/derp1'
  root2 = '/var/lib/derp2'
  fpd1 = FixedPathDetector(path=root1)
  fpd2 = FixedPathDetector(path=root2)
  cpd = ChainedPathDetector(fpd1, fpd2)
  assert set(cpd.get_paths()) == set([root1, root2])


def test_chained_path_detector_constructor():
  with pytest.raises(TypeError):
    ChainedPathDetector(1, 2, 3)

  with pytest.raises(TypeError):
    ChainedPathDetector(FixedPathDetector(), 'hello')
