import os

from twitter.thermos.base.path import TaskPath


def test_legacy_task_roots():
  assert TaskPath().given(task_id='foo').getpath('checkpoint_path').startswith(
      TaskPath.DEFAULT_CHECKPOINT_ROOT)
  assert TaskPath(root='/var/lib/mesos').given(task_id='foo').getpath('checkpoint_path').startswith(
      '/var/lib/mesos')


def test_legacy_log_dirs():
  assert TaskPath().given(task_id='foo').getpath('process_logbase') == os.path.join(
      TaskPath.DEFAULT_CHECKPOINT_ROOT, 'logs', 'foo')
  assert TaskPath(log_dir='sloth_love_chunk').given(task_id='foo').getpath(
      'process_logbase') == 'sloth_love_chunk'
