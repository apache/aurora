import copy
from contextlib import contextmanager

from twitter.common.contextutil import temporary_file
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import UpdateConfig
from twitter.mesos.parsers.pystachio_codec import PystachioCodec
from twitter.thermos.config.schema import Resources, Process, Task

from pystachio import String, Integer, Map, Empty


HELLO_WORLD = {
  'name': 'hello_world',
  'role': 'john_doe',
  'cluster': 'smf1-test',
  'task': {
    'start_command': 'echo hello world',
    'num_cpus': 0.1,
    'ram_mb': 64,
    'disk_mb': 64
  }
}


@contextmanager
def temporary_config(config):
  with temporary_file() as fp:
    fp.write('HAXXATTAXX = %r\n' % config)
    fp.write('jobs = [HAXXATTAXX]\n')
    fp.flush()
    yield fp


def convert(config):
  with temporary_config(config) as fp:
    codec = PystachioCodec(fp.name)
    return codec.build() % Cluster.get(config['cluster']).context()


def test_simple_config():
  job = convert(HELLO_WORLD)
  assert job.name() == String('hello_world')

  # properly converted defaults
  assert job.cluster() == String('smf1-test')
  assert job.instances() == Integer(1)
  assert job.cron_schedule() == String('')
  assert job.cron_policy() == String('KILL_EXISTING')
  assert job.update_config() == UpdateConfig(
    batch_size = 1,
    restart_threshold = 30,
    watch_secs = 30,
    max_per_shard_failures = 0,
    max_total_failures = 0
  )
  assert job.daemon() == Integer(0)  # Boolean(False)
  assert job.constraints() == Map(String, String)({})
  assert job.production() == Integer(0)
  assert job.priority() == Integer(0)
  assert job.max_task_failures() == Integer(1)
  assert job.health_check_interval_secs() == Empty
  assert job.task() == Task(
    name = job.name(),
    resources = Resources(cpu = 0.1, ram = 64 * 1048576, disk = 64 * 1048576),
    processes = [Process(name = job.name(), cmdline = 'echo hello world')],
  )


def test_config_with_copy():
  hello_world_with_copy = copy.deepcopy(HELLO_WORLD)
  hello_world_with_copy['task']['hdfs_path'] = '/mesos/pkg/john_doe/package.zip'
  job = convert(hello_world_with_copy)
  assert len(list(job.task().processes())) == 2
  constraints = job.task().constraints()
  assert len(list(constraints)) == 1
  assert len(list(constraints[0].ordered())) == 1
  assert constraints[0].ordered()[0].first() == String('installer')
  assert constraints[0].ordered()[0].second() == job.name()


def test_config_with_nondefault_update_config():
  hello_world_copy = copy.deepcopy(HELLO_WORLD)
  hello_world_copy['updateConfig'] = {}
  hello_world_copy['updateConfig']['watchSecs'] = 60
  job = convert(hello_world_copy)
  assert job.update_config() == UpdateConfig(
    batch_size = 1,
    restart_threshold = 30,
    watch_secs = 60,
    max_per_shard_failures = 0,
    max_total_failures = 0
  )


def test_config_with_options():
  hwc = copy.deepcopy(HELLO_WORLD)
  hwc['task']['production'] = True
  hwc['task']['priority'] = 200
  hwc['task']['daemon'] = True
  hwc['task']['health_check_interval_secs'] = 30
  hwc['cron_collision_policy'] = 'RUN_OVERLAP'
  hwc['constraints'] = {
    'dedicated': 'your_mom',
    'cpu': 'x86_64'
  }
  job = convert(hwc)

  assert job.production() == Integer(1)
  assert job.priority() == Integer(200)
  assert job.daemon() == Integer(1)
  assert job.cron_policy() == String('RUN_OVERLAP')
  assert job.health_check_interval_secs() == Integer(30)
  assert 'cpu' in job.constraints()
  assert 'dedicated' in job.constraints()
  assert job.constraints()['cpu'] == String('x86_64')
  assert job.constraints()['dedicated'] == String('your_mom')


def test_config_with_ports():
  hwc = copy.deepcopy(HELLO_WORLD)

  hwc['task']['start_command'] = 'echo %port:http%'
  job = convert(hwc)
  main_process = [proc for proc in job.task().processes() if proc.name() == job.name()]
  assert len(main_process) == 1
  main_process = main_process[0]
  assert main_process.cmdline() == String("echo {{thermos.ports[http]}}")

  hwc['task']['start_command'] = 'echo %port:http% %port:admin% %port:http%'
  job = convert(hwc)
  main_process = [proc for proc in job.task().processes() if proc.name() == job.name()]
  assert len(main_process) == 1
  main_process = main_process[0]
  assert main_process.cmdline() == String(
      "echo {{thermos.ports[http]}} {{thermos.ports[admin]}} {{thermos.ports[http]}}")


def test_config_with_other_replacements():
  hwc = copy.deepcopy(HELLO_WORLD)
  hwc['task']['start_command'] = 'echo %shard_id% %task_id% %port:http%'
  job = convert(hwc)
  main_process = [proc for proc in job.task().processes() if proc.name() == job.name()]
  assert len(main_process) == 1
  main_process = main_process[0]
  assert main_process.cmdline() == String(
      "echo {{mesos.instance}} {{thermos.task_id}} {{thermos.ports[http]}}")
