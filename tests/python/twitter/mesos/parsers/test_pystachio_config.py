import pytest

from twitter.common.contextutil import temporary_file
from twitter.mesos.config.schema import (
  MesosJob,
  Process,
  Resources,
  Task)
from twitter.mesos.parsers.pystachio_config import MesosConfigLoader

from gen.twitter.mesos.ttypes import Identity

MESOS_CONFIG = """
HELLO_WORLD = MesosJob(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo hello world')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
jobs = [HELLO_WORLD]
"""

REIFIED_CONFIG = MesosJob(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo hello world')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)

EMPTY_MESOS_CONFIG = """
foo = MesosJob(name = "hello_world")
"""

BAD_MESOS_CONFIG = """
jobs = 1234
"""


def test_simple_config():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()

    job1 = MesosConfigLoader.load(fp.name)
    job2 = MesosConfigLoader.load(fp.name, name="hello_world")
    with pytest.raises(ValueError):
      MesosConfigLoader.load(fp.name, name="herpderp")

  assert job1 == REIFIED_CONFIG
  assert job2 == REIFIED_CONFIG


def test_bad_config():
  with temporary_file() as fp:
    fp.write(BAD_MESOS_CONFIG)
    fp.flush()
    with pytest.raises(MesosConfigLoader.BadConfig):
      MesosConfigLoader.load(fp.name)


def test_empty_config():
  with temporary_file() as fp:
    fp.write(EMPTY_MESOS_CONFIG)
    fp.flush()
    with pytest.raises(MesosConfigLoader.BadConfig):
      MesosConfigLoader.load(fp.name)
