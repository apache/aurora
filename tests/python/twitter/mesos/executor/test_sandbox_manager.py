import os
import pytest
from twitter.common.contextutil import temporary_dir
from twitter.mesos.config.schema import (
  Task,
  Resources,
  Layout,
  Package,
  MesosTaskInstance)

from twitter.mesos.executor.sandbox_manager import (
  SandboxManager,
  DirectorySandbox,
  AppAppSandbox)

DEFAULT_LAYOUT = Layout(packages = [Package(name="herpderp")])

TASK_ID = 'wickman-test-taskid-abcdefg'
MESOS_TASK = MesosTaskInstance(
  task = Task(name = "hello_world", resources = Resources(cpu=1,ram=1,disk=1)),
  instance = 0,
  role = 'herpderp',
  layout = DEFAULT_LAYOUT
)

def test_directory_sandbox():
  with temporary_dir() as d:
    ds = DirectorySandbox(TASK_ID, sandbox_root=d)
    assert ds.root() == os.path.join(d, TASK_ID)
    assert not os.path.exists(ds.root())
    ds.create(MESOS_TASK)
    assert os.path.exists(ds.root())
    ds.destroy()
    assert not os.path.exists(ds.root())
