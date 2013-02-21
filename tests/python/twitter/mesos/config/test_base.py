import pytest

from twitter.common.contextutil import temporary_file
from twitter.mesos.config import AuroraConfig, PortResolver
from twitter.mesos.config.schema import (
  Announcer,
  Empty,
  Integer,
  Job,
  Process,
  Resources,
  Task)

from gen.twitter.mesos.ttypes import Identity


resolve = PortResolver.resolve
unallocated = PortResolver.unallocated
bound = PortResolver.bound


def test_all_static():
  portmap = {}
  assert resolve(portmap) == {}
  assert bound(portmap) == set()
  assert unallocated(portmap) == set()

  portmap = {'port': '80'}
  assert resolve(portmap) == {'port': 80}
  assert bound(resolve(portmap)) == set(['port'])
  assert unallocated(resolve(portmap)) == set()


def test_binding():
  portmap = {'aurora': 'http', 'http': 80}
  assert resolve(portmap) == {'aurora': 80, 'http': 80}
  assert unallocated(resolve(portmap)) == set()
  assert bound(resolve(portmap)) == set(['aurora', 'http'])

  portmap = {'aurora': 'http', 'http': 'unbound'}
  assert resolve(portmap) == {'aurora': 'unbound', 'http': 'unbound'}
  assert unallocated(resolve(portmap)) == set(['unbound'])
  assert bound(resolve(portmap)) == set(['aurora', 'http'])


def test_cycle():
  portmap = {'aurora': 'http', 'http': 'aurora'}
  with pytest.raises(PortResolver.CycleException):
    resolve(portmap)
  portmap = {'aurora': 'http', 'http': 'https', 'https': 'aurora'}
  with pytest.raises(PortResolver.CycleException):
    resolve(portmap)



MESOS_CONFIG = """
HELLO_WORLD = Job(
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

LIMITED_MESOS_CONFIG = """
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world_fails_0', cmdline = 'echo hello world',
                         max_failures = 0),
                 Process(name = 'hello_world_fails_50', cmdline = 'echo hello world',
                         max_failures = 50),
                 Process(name = 'hello_world_fails_100', cmdline = 'echo hello world',
                         max_failures = 100),
                 Process(name = 'hello_world_fails_200', cmdline = 'echo hello world',
                         max_failures = 200)],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)
jobs = [HELLO_WORLD]
"""


REIFIED_CONFIG = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world', cmdline = 'echo {{mesos.instance}}')],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)

REIFIED_LIMITED_CONFIG = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    processes = [Process(name = 'hello_world_fails_0', cmdline = 'echo hello world',
                         max_failures = 0),
                 Process(name = 'hello_world_fails_50', cmdline = 'echo hello world',
                         max_failures = 50),
                 Process(name = 'hello_world_fails_100', cmdline = 'echo hello world',
                         max_failures = 100),
                 Process(name = 'hello_world_fails_200', cmdline = 'echo hello world',
                         max_failures = 200)],
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
  )
)

EMPTY_MESOS_CONFIG = """
foo = Job(name = "hello_world")
"""

UNDERSPECIFIED_MESOS_CONFIG = """
jobs = [
  Job(name = "hello_world")
]
"""

BAD_MESOS_CONFIG = """
jobs = 1234
"""


def test_empty_config():
  with pytest.raises(AuroraConfig.InvalidConfig):
    with temporary_file() as fp:
      fp.write(UNDERSPECIFIED_MESOS_CONFIG)
      fp.flush()
      AuroraConfig.load(fp.name)


def test_simple_config():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    proxy_config1 = AuroraConfig.load(fp.name)
    proxy_config2 = AuroraConfig.load(fp.name, name="hello_world")
    assert proxy_config1._job == proxy_config2._job
    assert proxy_config1._job == REIFIED_CONFIG
    assert proxy_config1.name() == 'hello_world'
    assert proxy_config1.role() == 'john_doe'
    assert proxy_config1.cluster() == 'smf1-test'
    assert proxy_config1.ports() == set()


def test_limited_config():
  with temporary_file() as fp:
    fp.write(LIMITED_MESOS_CONFIG)
    fp.flush()

    job = AuroraConfig.load(fp.name)
    assert job != REIFIED_LIMITED_CONFIG

    process_map = dict((str(process.name()), process) for process in job._job.task().processes())
    assert len(process_map) == 4
    for process in ('hello_world_fails_0', 'hello_world_fails_50', 'hello_world_fails_100',
                    'hello_world_fails_200'):
      assert process in process_map
    assert process_map['hello_world_fails_0'].max_failures() == Integer(100)
    assert process_map['hello_world_fails_50'].max_failures() == Integer(50)
    assert process_map['hello_world_fails_100'].max_failures() == Integer(100)
    assert process_map['hello_world_fails_200'].max_failures() == Integer(100)


def test_ports():
  def make_config(announce, *ports):
    process = Process(name = 'hello',
                      cmdline = ' '.join('{{thermos.ports[%s]}}' % port for port in ports))
    return AuroraConfig(Job(
        name = 'hello_world', role = 'john_doe', cluster = 'smf1-test',
        announce = announce,
        task = Task(name = 'main', processes = [process],
                    resources =  Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576))))

  announce = Announcer(portmap = {'http': 80})
  assert make_config(announce).ports() == set()
  assert make_config(announce, 'http').ports() == set()
  assert make_config(announce, 'http', 'thrift').ports() == set(['thrift'])

  announce = Announcer(portmap = {'http': 'aurora'})
  assert make_config(announce).ports() == set(['aurora'])
  assert make_config(announce, 'http').ports() == set(['aurora'])
  assert make_config(announce, 'http', 'thrift').ports() == set(['thrift', 'aurora'])

  announce = Announcer(portmap = {'aurora': 'http'})
  assert make_config(announce).ports() == set(['http'])
  assert make_config(announce, 'http').ports() == set(['http'])
  assert make_config(announce, 'http', 'thrift').ports() == set(['http', 'thrift'])

  assert make_config(Empty).ports() == set()
  assert make_config(Empty, 'http').ports() == set(['http'])
  assert make_config(Empty, 'http', 'thrift').ports() == set(['http', 'thrift'])
