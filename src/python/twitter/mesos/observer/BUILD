python_library(
  name = 'mesos_vars',
  sources = ['mesos_vars.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/common/python'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/string'),
    pants('src/python/twitter/mesos/executor:executor_detector'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ],
)

python_binary(
  name = 'thermos_observe',
  source = 'thermos_observe.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/app/modules:http'),
    pants('src/python/twitter/common/app/modules:vars'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
    pants('src/python/twitter/mesos/executor:resource_checkpoints'),
    pants('src/python/twitter/mesos/common_internal:clusters'),
    pants('src/python/twitter/thermos/base:path'),
    pants('src/python/twitter/thermos/observer'),
    pants(':mesos_vars'),
  ],
)
