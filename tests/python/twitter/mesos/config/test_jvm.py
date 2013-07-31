from twitter.common.contextutil import temporary_file
from twitter.mesos.config import AuroraConfig

MESOS_CONFIG_DEFAULT = """
RESOURCES = Resources(cpu = 1.0, ram = 256*MB, disk = 128*MB)
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    resources = RESOURCES,
    processes = [JVMProcess(arguments = ' -version', resources = RESOURCES)],
  )
)
jobs = [HELLO_WORLD]
"""

MESOS_CONFIG_ELLIPSIS = """
RESOURCES = Resources(cpu = 1.0, ram = 256*MB, disk = 128*MB)
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    resources = RESOURCES,
    processes = [JVMProcess(arguments = ' -version', resources = RESOURCES, name='Bird', jvm = Java7(java_home = '/usr/lib/jvm/java-1.7.0-openjdk', collector = 'latency', heap = '228', new_gen = '114', perm_gen = '32', code_cache = '24', cpu_cores = '1', gc_log = 'gctest.log', extra_jvm_flags = '-XX:+PrintFlagsFinal' ), max_failures = 0, ephemeral = False)],
  )
)
jobs = [HELLO_WORLD]
"""

MESOS_CONFIG = """
RESOURCES = Resources(cpu = 1.0, ram = 256*MB, disk = 128*MB)
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    resources = RESOURCES,
    processes = [JVMProcess(arguments = ' -version', resources = RESOURCES, jvm = Java7(java_home = '/usr/lib/jvm/java-1.7.0-openjdk', collector = 'latency', heap = '228', new_gen = '114', perm_gen = '32', code_cache = '24', cpu_cores = '1', gc_log = 'gctest.log', extra_jvm_flags = '-XX:+PrintFlagsFinal' ))],
  )
)
jobs = [HELLO_WORLD]
"""

MESOS_CONFIG_JAVA6 = """
RESOURCES = Resources(cpu = 1.0, ram = 256*MB, disk = 128*MB)
HELLO_WORLD = Job(
  name = 'hello_world',
  role = 'john_doe',
  cluster = 'smf1-test',
  task = Task(
    name = 'main',
    resources = RESOURCES,
    processes = [JVMProcess(arguments = ' -version', resources = RESOURCES, jvm = Java6(java_home = '/usr/lib/jvm/java-1.6.0', collector = 'latency', heap = '228', new_gen = '114', perm_gen = '32', code_cache = '24', cpu_cores = '1', gc_log = 'gctest.log', extra_jvm_flags = '-XX:+PrintFlagsFinal' ))],
  )
)
jobs = [HELLO_WORLD]
"""

def test_default_jvm_config():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG_DEFAULT)
    fp.flush()
    proxy_config1 = AuroraConfig.load(fp.name)
    assert proxy_config1.name() == 'hello_world'
    assert proxy_config1.role() == 'john_doe'
    assert proxy_config1.cluster() == 'smf1-test'

def test_jvm_config_with_ellipsis():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG_ELLIPSIS)
    fp.flush()
    proxy_config1 = AuroraConfig.load(fp.name)
    assert proxy_config1.name() == 'hello_world'
    assert proxy_config1.role() == 'john_doe'
    assert proxy_config1.cluster() == 'smf1-test'

def test_simple_jvm_config():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG)
    fp.flush()
    proxy_config1 = AuroraConfig.load(fp.name)
    assert proxy_config1.name() == 'hello_world'
    assert proxy_config1.role() == 'john_doe'
    assert proxy_config1.cluster() == 'smf1-test'

def test_simple_jvm_config_java6():
  with temporary_file() as fp:
    fp.write(MESOS_CONFIG_JAVA6)
    fp.flush()
    proxy_config1 = AuroraConfig.load(fp.name)
    assert proxy_config1.name() == 'hello_world'
    assert proxy_config1.role() == 'john_doe'
    assert proxy_config1.cluster() == 'smf1-test'
