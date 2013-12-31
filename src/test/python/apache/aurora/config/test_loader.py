import json
import tempfile

from twitter.common.contextutil import temporary_file

from twitter.aurora.config import AuroraConfig
from twitter.aurora.config.loader import AuroraConfigLoader
from twitter.thermos.config.loader import ThermosTaskWrapper

from pystachio import Environment
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

def test_enoent():
  nonexistent_file = tempfile.mktemp()
  with pytest.raises(AuroraConfigLoader.NotFound):
    AuroraConfigLoader.load(nonexistent_file)


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


def test_load():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    fp.seek(0)

    for config in (fp.name, fp):
      env = AuroraConfigLoader.load(config)
      assert 'jobs' in env and len(env['jobs']) == 1
      hello_world = env['jobs'][0]
      assert hello_world.name().get() == 'hello_world'


def test_pick():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    env = AuroraConfigLoader.load(fp.name)

  hello_world = env['jobs'][0]
  assert AuroraConfig.pick(env, 'hello_world', None) == hello_world

  env['jobs'][0] = env['jobs'][0](name = 'something_{{else}}')
  assert str(AuroraConfig.pick(env, 'something_else', [{'else': 'else'}]).name()) == (
      'something_else')
