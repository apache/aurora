python_library(
  name = 'sandbox_manager',
  sources = ['sandbox_manager.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('3rdparty/python.twitter:appapp')
  ]
)

python_library(
  name = 'task_runner_wrapper',
  sources = ['task_runner_wrapper.py'],
  dependencies = [
    pants(':sandbox_manager'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/http'),
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:runner'),
    pants('src/python/twitter/thermos:observer'),
    pants('src/python/twitter/thermos/config:schema'),
  ]
)

python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor.py',
  dependencies = [
    pants(':task_runner_wrapper'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('3rdparty/python.twitter:mesos-latest')
  ]
)
