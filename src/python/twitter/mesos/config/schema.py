from pystachio import *
from twitter.thermos.config.schema import (
  Task,
  Process,
  Resources,
  ThermosContext)


class Package(Struct):
  name = Required(String)
  version = Default(String, 'latest')

class Service(Struct):
  name = Required(String)
  config = String

class Layout(Struct):
  packages = List(Package)
  services = List(Service)

DEFAULT_LAYOUT = Layout(
  packages = [
    Package(name = "centos-5.5-x86_64"),
    Package(name = "centos-5.5-util-x86_64"),
  ]
)


"""
Example from the tutorial

fizzbuzzer = {
  'name': 'fizzbuzzer',
  'role': 'john_doe',
  'cluster': 'smf1-foobar',
  'instances': 2,
  'task': {
     'daemon': True,
     'hdfs_path': '/user/mesos/john_doe/fizzbuzzer.tar.gz',
     'start_command': 'tar xzf fizzbuzzer.tar.gz; sh run_fizzbuzzer.sh',
     'num_cpus': 1,
     'ram_mb': 1024,
     'disk_mb': 1024
   },
   'update_config': {
     'batchSize': 3,
     'restartThreshold': 10,
     'watchSecs': 30,
     'maxPerShardFailures': 0,
     'maxTotalFailures': 0
   }
}

buzzfizzer = {
  'name': 'buzzfizzer',
  'role': 'john_doe',
  'cluster': 'smf1-foobar',
  'task': {
     'hdfs_path': '/user/mesos/john_doe/buzzfizzer.tar.gz',
     'start_command': 'tar xzf buzzfizzer.tar.gz; sh buzzfizzer.sh'
  }
}

jobs = [fizzbuzzer, buzzfizzer]
"""


class MesosContext(Struct):
  role = String
  cluster = String

class UpdateConfig(Struct):
  batch_size         = Default(Integer, 3)
  restart_threshold  = Default(Integer, 10)
  watch_secs         = Default(Integer, 30)
  failures_per_shard = Default(Integer, 0)
  total_failures     = Default(Integer, 0)


@Provided(mesos = MesosContext)
class MesosJob(Struct):
  name          = Required(String)
  role          = Default(String, '{{mesos.role}}')
  cluster       = Default(String, '{{mesos.cluster}}')
  instances     = Default(Integer, 1)
  task          = Required(Task)
  layout        = Default(Layout, DEFAULT_LAYOUT)
  update_config = Default(UpdateConfig, UpdateConfig())
