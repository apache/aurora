from pystachio import *
from twitter.thermos.config.schema import (
  Task,
  Process,
  Resources,
  ThermosContext)

class Package(Struct):
  name    = Required(String)
  version = Default(String, 'latest')

class Service(Struct):
  name   = Required(String)
  config = String

class Layout(Struct):
  packages = Default(List(Package), [])
  services = Default(List(Service), [])

DEFAULT_LAYOUT = Layout(
  packages=[
    Package(name="centos-5.5-x86_64"),
    Package(name="centos-5.5-util-x86_64"),
  ]
)

class MesosContext(Struct):
  role     = Required(String)
  cluster  = Required(String)
  instance = Required(Integer)

class UpdateConfig(Struct):
  batch_size         = Default(Integer, 3)
  restart_threshold  = Default(Integer, 10)
  watch_secs         = Default(Integer, 30)
  failures_per_shard = Default(Integer, 0)
  total_failures     = Default(Integer, 0)

# The thermosConfig populated inside of TwitterTaskInfo.
@Provided(mesos = MesosContext)
class MesosTaskInstance(Struct):
  task     = Required(Task)
  layout   = Required(Layout)
  instance = Required(Integer)
  role     = Required(String)

@Provided(mesos=MesosContext)
class MesosJob(Struct):
  name          = Required(String)
  role          = Default(String, '{{mesos.role}}')
  cluster       = Default(String, '{{mesos.cluster}}')
  instances     = Default(Integer, 1)
  task          = Required(Task)
  cron_schedule = String
  cron_policy   = Default(String, 'KILL_EXISTING')
  layout        = Default(Layout, DEFAULT_LAYOUT)
  update_config = Default(UpdateConfig, UpdateConfig())
