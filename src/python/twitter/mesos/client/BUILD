python_library(
  name = 'api',
  sources = ['api.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants(':updater'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'client',
  dependencies = [
    pants(':api'),
    pants(':base'),
    pants(':command_runner'),
    pants(':config'),
    pants(':quickrun'),
  ]
)

python_library(
  name = 'base',
  sources = ['base.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'config',
  sources = ['config.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/mesos/config'),
    pants('src/python/twitter/mesos/packer'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'command_runner',
  sources = ['command_runner.py'],
  dependencies = [
    pants(':api'),
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'job_monitor',
  sources = ['job_monitor.py'],
  dependencies = [
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
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
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'scheduler_client',
  sources = ['scheduler_client.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants('src/python/twitter/common_internal/auth'),
    pants('src/python/twitter/common_internal/location'),
    pants('src/python/twitter/common_internal/zookeeper:tunneler'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
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
    pants('src/python/twitter/mesos/executor:thermos_executor_lib'),
    pants('src/python/twitter/mesos/executor:task_runner_wrapper'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
    pants('src/python/twitter/thermos/observer'),
    python_requirement('cherrypy'),
    python_requirement('mesos'),
  ]
)

python_library(
  name = 'updater',
  sources = ['updater.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)
