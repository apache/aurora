python_library(
  name = 'api',
  sources = ['__init__.py'],
  dependencies = [
    pants(':restarter'),
    pants(':scheduler_client'),
    pants(':updater'),
    pants('src/python/twitter/aurora/common'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'command_runner',
  sources = ['command_runner.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':api'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/config'),
    pants('src/python/twitter/aurora/common:cluster'),
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'disambiguator',
  sources = ['disambiguator.py'],
  dependencies = [
    pants(':api'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/aurora/common'),
    pants('src/python/twitter/common/log'),
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
  name = 'restarter',
  sources = ['restarter.py'],
  dependencies = [
    pants(':instance_watcher'),
    pants(':updater_util'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'scheduler_client',
  sources = ['scheduler_client.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common/auth'),
    pants('src/python/twitter/aurora/common:cluster'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/rpc/transports:tsslsocket'),
    pants('src/python/twitter/common/zookeeper/serverset:kazoo_serverset'),
    pants('src/python/twitter/common/zookeeper:kazoo_client'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'instance_watcher',
  sources = ['instance_watcher.py', 'health_check.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants('src/python/twitter/aurora/common:http_signaler'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'updater',
  sources = ['updater.py'],
  dependencies = [
    pants(':scheduler_client'),
    pants(':instance_watcher'),
    pants(':updater_util'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'updater_util',
  sources = ['updater_util.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
  ]
)
