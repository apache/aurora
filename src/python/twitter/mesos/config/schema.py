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
  # DEPRECATED in favor of using {{packer[role][package][version].package}}
  package     = String

  # The HDFS URI of the package associated with this job
  # DEPRECATED in favor of using {{packer[role][package][version].package_uri}}
  package_uri = String


# AppApp layout setup
class AppPackage(Struct):
  name    = Required(String)
  version = Default(String, 'latest')

class AppLayout(Struct):
  packages = Default(List(AppPackage), [])


# The object bound into the {{packer}} namespace.
# Referenced by
#  {{packer[role][name][version]}}
#
# Where version =
#    number (integer)
#    'live' (live package)
#    'latest' (highest version number)
#
# For example if you'd like to create a copy process for a particular
# package,
#   copy_latest = Process(
#     name = 'copy-{{package_name}}',
#     cmdline = '{{packer[{{role}}][{{package_name}}][latest].copy_command}}')
#   processes = [
#     copy_latest.bind(package_name = 'labrat'),
#     copy_latest.bind(package_name = 'packer')
#   ]
class Packer(Struct):
  package = String
  package_uri = String

  # 'copy_command' is bound to the command in the context
  copy_command = String

  tunnel_host = Default(String, 'nest2.corp.twitter.com')
  local_copy_command = Default(String,
      'ssh {{tunnel_host}} hadoop fs -cat {{package_uri}} > {{package}}')
  remote_copy_command = Default(String,
      'hadoop fs -copyToLocal {{package_uri}} {{package}}')


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



class Announcer(Struct):
  environment  = Default(String, 'prod')
  strict       = Default(Integer, False)
  primary_port = Default(String, 'http')


# The thermosConfig populated inside of TwitterTaskInfo.
class MesosTaskInstance(Struct):
  task                       = Required(Task)
  layout                     = AppLayout
  instance                   = Required(Integer)
  role                       = Required(String)
  announce                   = Announcer
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
  package       = PackerPackage  # DEPRECATED in favor of {{packer}} namespaces.
  announce      = Announcer

  update_config = Default(UpdateConfig, UpdateConfig())

  constraints                = Map(String, String)
  daemon                     = Default(Integer, 0)  # boolean
  max_task_failures          = Default(Integer, 1)
  production                 = Default(Integer, 0)  # boolean
  priority                   = Default(Integer, 0)
  health_check_interval_secs = Default(Integer, 30)
  task_links                 = Map(String, String)


Job = MesosJob
