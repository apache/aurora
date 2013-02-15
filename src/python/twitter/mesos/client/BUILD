python_library(
  name = 'job_monitor',
  sources = ['job_monitor.py'],
  dependencies = [
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'quickrun',
  sources = ['quickrun.py'],
  dependencies = [
    pants(':client_util'),
    pants(':job_monitor'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos:parsers'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'client_util',
  sources = ['client_util.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':client_wrapper'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/packer'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/mesos:parsers'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'client_wrapper',
  sources = ['client_wrapper.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/mesos:scheduler_client'),
    pants('src/python/twitter/mesos:session_key_helper'),
    pants('src/python/twitter/mesos:updater'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'spawn_local',
  sources = ['spawn_local.py'],
  dependencies = [
    pants(':client_util'),
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
  name = 'client',
  dependencies = [
    pants(':client_util'),
    pants(':client_wrapper'),
    pants(':quickrun'),
  ]
)
