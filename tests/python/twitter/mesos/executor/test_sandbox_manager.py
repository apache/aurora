import os
import pwd
import pytest
from twitter.common.contextutil import temporary_dir
from twitter.mesos.config.schema import (
  Task,
  Resources,
  AppLayout,
  AppPackage,
  MesosTaskInstance)

from twitter.mesos.executor.sandbox_manager import (
  SandboxManager,
  DirectorySandbox,
  AppAppSandbox)

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
    ds = DirectorySandbox(TASK_ID, sandbox_root=d)
    assert ds.root() == os.path.join(d, TASK_ID)
    assert not os.path.exists(ds.root())
    ds.create(MESOS_TASK)
    assert os.path.exists(ds.root())
    root_stat = os.stat(ds.root())
    assert root_stat.st_uid == os.getuid()
    assert root_stat.st_mode & 0777 == 0700  # chmod 700
    ds.destroy()
    assert not os.path.exists(ds.root())
