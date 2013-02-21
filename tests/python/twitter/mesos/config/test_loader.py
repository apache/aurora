import json

from twitter.common.contextutil import temporary_file

from twitter.mesos.config.loader import AuroraConfigLoader
from twitter.thermos.config.loader import ThermosTaskWrapper

import pytest


BAD_MESOS_CONFIG = """
3 2 1 3 2 4 2 3
"""

MESOS_CONFIG = """
HELLO_WORLD = MesosJob(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
jobs = [HELLO_WORLD]
"""


def test_bad_config():
  with temporary_file() as fp:
    fp.write(BAD_MESOS_CONFIG)
    fp.flush()
    with pytest.raises(AuroraConfigLoader.InvalidConfigError):
      AuroraConfigLoader.load(fp.name)


def test_empty_config():
  with temporary_file() as fp:
    fp.flush()
    AuroraConfigLoader.load(fp.name)


def test_load_json():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    env = AuroraConfigLoader.load(fp.name)
    job = env['jobs'][0]
  with temporary_file() as fp:
    fp.write(json.dumps(job.get()))
    fp.flush()
    new_job = AuroraConfigLoader.load_json(fp.name)
    assert new_job == job


def test_load_into():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    env = AuroraConfigLoader.load_into(fp.name)
    assert 'jobs' in env and len(env['jobs']) == 1
    hello_world = env['jobs'][0]
    assert hello_world.name().get() == 'hello_world'
