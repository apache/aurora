import os
import pwd
import pytest

from twitter.common.contextutil import temporary_dir

from twitter.aurora.config.schema.base import (
  Task,
  Resources,
  AppLayout,
  AppPackage,
  MesosTaskInstance,
)
from twitter.aurora.executor.sandbox_manager import DirectorySandbox


DEFAULT_LAYOUT = AppLayout(packages = [AppPackage(name="herpderp")])

TASK_ID = 'wickman-test-taskid-abcdefg'
MESOS_TASK = MesosTaskInstance(
  task = Task(name = "hello_world", resources = Resources(cpu=1,ram=1,disk=1)),
  instance = 0,
  role = pwd.getpwuid(os.getuid()).pw_name,
  layout = DEFAULT_LAYOUT
)


def test_directory_sandbox():
  with temporary_dir() as d:
    ds1 = DirectorySandbox(os.path.join(d, 'task1'))
    ds2 = DirectorySandbox(os.path.join(d, 'task2'))
    ds1.create(MESOS_TASK)
    ds2.create(MESOS_TASK)
    assert os.path.exists(ds1.root)
    assert os.path.exists(ds2.root)
    ds1.destroy()
    assert not os.path.exists(ds1.root)
    assert os.path.exists(ds2.root)
    ds2.destroy()
    assert not os.path.exists(ds2.root)
