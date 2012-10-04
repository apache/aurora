python_library(
  name = 'parsers',
  sources = globs('parsers/*.py'),
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants(':clusters')
  ]
)

python_library(
  name = 'clusters',
  sources = ['clusters.py'],
)

python_library(
  name = 'location',
  sources = ['location.py'],
)

python_library(
  name = 'zookeeper_helper',
  sources = ['zookeeper_helper.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/zookeeper'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants(':clusters'),
    pants(':location'),
  ]
)

python_library(
  name = 'updater',
  sources = ['updater.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'scheduler_client',
  sources = ['scheduler_client.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants(':clusters'),
    pants(':location'),
    pants(':zookeeper_helper'),
  ]
)

python_library(
  name = 'session_key_helper',
  sources = ['session_key_helper.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/python/twitter/common_internal/ods'),
    pants('3rdparty/python:paramiko'),
  ]
)

python_library(
  name = 'spawn_local',
  sources = ['spawn_local.py'],
  dependencies = [
    python_requirement('mesos==0.9.0'),
    python_requirement('thrift'),
    python_requirement('tornado'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
    pants('src/python/twitter/mesos/executor:task_runner_wrapper'),
    pants('src/python/twitter/mesos/executor:thermos_executor_lib'),
    pants('src/python/twitter/thermos/observer'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'client_wrapper',
  sources = ['client_wrapper.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants(':clusters'),
    pants(':location'),
    pants(':scheduler_client'),
    pants(':session_key_helper'),
    pants(':updater'),
  ]
)

python_library(
  name = 'command_runner',
  sources = ['command_runner.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':client_wrapper'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'mesos-lib',
  dependencies = [
    pants('src/python/twitter/mesos/packer'),
    pants(':clusters'),
    pants(':command_runner'),
    pants(':location'),
    pants(':parsers'),
    pants(':scheduler_client'),
    pants(':client_wrapper'),
    pants(':zookeeper_helper'),
  ]
)

python_binary(
  name = 'mesos_client',
  source = 'bin/mesos_client.py',
  dependencies = [
    pants(':mesos-lib'),
    pants(':spawn_local'),
    pants('src/python/twitter/thermos/observer'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/base:options')
  ],
)

python_binary(
  name = 'deploy_scheduler',
  source = 'bin/deploy_scheduler.py',
  dependencies = [
    pants(':clusters'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/mesos/deploy'),
  ]
)

python_binary(
  name = 'deploy_thermos',
  source = 'bin/deploy_thermos.py',
  dependencies = [
    pants(':clusters'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/mesos/deploy')
  ]
)

python_binary(
  name = 'process_scraper',
  source = 'process_scraper.py',
  dependencies = [
    python_requirement('psutil>=0.5.0'),
    pants('src/python/twitter/common/app'),
  ],
)
