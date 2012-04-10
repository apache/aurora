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

class MesosContext(Struct):
  role     = Required(String)
  cluster  = Required(String)
  instance = Required(Integer)

class UpdateConfig(Struct):
  batch_size             = Default(Integer, 1)
  restart_threshold      = Default(Integer, 30)
  watch_secs             = Default(Integer, 30)
  max_per_shard_failures = Default(Integer, 0)
  max_total_failures     = Default(Integer, 0)

# The thermosConfig populated inside of TwitterTaskInfo.
@Provided(mesos=MesosContext)
class MesosTaskInstance(Struct):
  task     = Required(Task)
  layout   = Layout
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
  layout        = Layout
  update_config = Default(UpdateConfig, UpdateConfig())

  # NEW
  constraints       = Map(String, String)
  daemon            = Default(Integer, 0)  # boolean
  max_task_failures = Default(Integer, 1)
  production        = Default(Integer, 0)  # boolean
  priority          = Default(Integer, 0)
  health_check_interval_secs = Integer
