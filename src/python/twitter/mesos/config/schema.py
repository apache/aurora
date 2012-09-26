from pystachio import *
from twitter.thermos.config.schema import *

class MesosContext(Struct):
  # The role running the job
  role        = Required(String)

  # The cluster in which the job is running
  cluster     = Required(String)

  # The instance id (i.e. replica id, shard id) in the context of a task
  instance    = Required(Integer)

  # The filename of the package associated with this job
  package     = String

  # The HDFS URI of the package associated with this job
  package_uri = String


# AppApp layout setup
class AppPackage(Struct):
  name    = Required(String)
  version = Default(String, 'latest')

class AppService(Struct):
  name   = Required(String)
  config = String

class AppLayout(Struct):
  packages = Default(List(AppPackage), [])
  services = Default(List(AppService), [])


# Packer package information
@Provided(mesos=MesosContext)
class PackerPackage(Struct):
  name = Required(String)
  role = Default(String, '{{mesos.role}}')
  version = Required(String)

Package = PackerPackage


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
  layout   = AppLayout
  instance = Required(Integer)
  role     = Required(String)
  health_check_interval_secs = Integer


class MesosJob(Struct):
  name          = Default(String, '{{task.name}}')
  role          = Required(String)
  cluster       = Required(String)
  instances     = Default(Integer, 1)
  task          = Required(Task)
  cron_schedule = String
  cron_policy   = Default(String, 'KILL_EXISTING')
  layout        = AppLayout
  package       = PackerPackage
  update_config = Default(UpdateConfig, UpdateConfig())

  constraints       = Map(String, String)
  daemon            = Default(Integer, 0)  # boolean
  max_task_failures = Default(Integer, 1)
  production        = Default(Integer, 0)  # boolean
  priority          = Default(Integer, 0)
  health_check_interval_secs = Default(Integer, 30)


Job = MesosJob
