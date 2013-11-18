python_library(
  name = 'all',
  dependencies = [
    pants(':core'),
    pants(':help'),
    pants(':run'),
    pants(':ssh'),
  ]
)

python_library(
  name = 'admin',
  sources = ['admin.py'],
  dependencies = [
    pants('src/python/twitter/aurora/admin:mesos_maintenance'),
    pants('src/python/twitter/aurora/client/api'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/common:clusters'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'core',
  sources = ['core.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client/api:command_runner'),
    pants('src/python/twitter/aurora/client/api:disambiguator'),
    pants('src/python/twitter/aurora/client/api:job_monitor'),
    pants('src/python/twitter/aurora/client/api:updater'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/client:config'),
    pants('src/python/twitter/aurora/client:factory'),
    pants('src/python/twitter/aurora/client:options'),
    pants('src/python/twitter/aurora/client/hooks'),
    pants('src/python/twitter/aurora/common'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/python'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'help',
  sources = ['help.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/common/app'),
  ]
)

python_library(
  name = 'run',
  sources = ['run.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client/api:command_runner'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/client:options'),
    pants('src/python/twitter/aurora/common:aurora_job_key'),
    pants('src/python/twitter/aurora/common:clusters'),
    pants('src/python/twitter/common/app'),
  ]
)

python_library(
  name = 'ssh',
  sources = ['ssh.py'],
  dependencies = [
    pants('src/python/twitter/aurora/client/api:command_runner'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/client:factory'),
    pants('src/python/twitter/aurora/client:options'),
    pants('src/python/twitter/aurora/common:aurora_job_key'),
    pants('src/python/twitter/aurora/common:clusters'),
    pants('src/python/twitter/common/app'),
  ]
)
