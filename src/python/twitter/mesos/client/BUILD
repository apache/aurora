python_library(
  name = 'api',
  sources = ['api.py'],
  dependencies = [
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':updater'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/common'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'client',
  dependencies = [
    pants(':api'),
    pants(':base'),
    pants(':command_runner'),
    pants(':config'),
    pants(':disambiguator'),
    pants(':quickrun'),
  ]
)

# Ideally these would be resources but the pkg_resources module loading mechanism needs
# sentinel __init__.py files written and those do not get written for non-"source" files.
python_library(
  name = 'recipes',
  sources = globs('recipes/*')
)

python_library(
  name = 'base',
  sources = ['base.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'config',
  sources = ['config.py'],
  dependencies = [
    pants(':base'),
    pants(':build'),
    pants(':jenkins'),
    pants(':recipes'),
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/config'),
    pants('src/python/twitter/mesos/packer'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'command_runner',
  sources = ['command_runner.py'],
  dependencies = [
    pants(':api'),
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/mesos/common:cluster'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'disambiguator',
  sources = ['disambiguator.py'],
  dependencies = [
    pants(':api'),
    pants(':base'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/common')
  ]
)

python_library(
  name = 'job_monitor',
  sources = ['job_monitor.py'],
  dependencies = [
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'quickrun',
  sources = ['quickrun.py'],
  dependencies = [
    pants(':config'),
    pants(':job_monitor'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/config'),
    pants('src/python/twitter/mesos/common:aurora_job_key'),
    pants('src/python/twitter/mesos/packer'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'restarter',
  sources = ['restarter.py'],
  dependencies = [
    pants(':shard_watcher'),
    pants(':updater_util'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'scheduler_client',
  sources = ['scheduler_client.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants('src/python/twitter/common_internal/location'),
    pants('src/python/twitter/common_internal/zookeeper:tunneler'),
    pants('src/python/twitter/mesos/common/auth'),
    pants('src/python/twitter/mesos/common:cluster'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'spawn_local',
  sources = ['spawn_local.py'],
  dependencies = [
    pants(':base'),
    pants(':config'),
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/common/http'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:tunnel'),
    pants('src/python/twitter/mesos:mesos-core'),
    pants('src/python/twitter/mesos/executor:thermos_executor_lib'),
    pants('src/python/twitter/mesos/executor:task_runner_wrapper'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
    pants('src/python/twitter/thermos/observer'),
    python_requirement('cherrypy'),
  ]
)

python_library(
  name = 'shard_watcher',
  sources = ['shard_watcher.py', 'health_check.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/common:http_signaler'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'updater',
  sources = ['updater.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants(':shard_watcher'),
    pants(':updater_util'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

# Jenkins Artifact Resolver
python_library(
  name = 'jenkins',
  sources = ['jenkins.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('3rdparty/python:jenkinsapi'),
  ]
)

python_library(
  name = 'updater_util',
  sources = ['updater_util.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
  ]
)

# Build Artifact Resolver
python_library(
  name = 'build',
  sources = ['build.py'],
  dependencies = [
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/git'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
  ]
)
