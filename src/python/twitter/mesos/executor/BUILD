python_library(
  name = 'thermos_lib',
  dependencies = [
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:runner'),
    pants('src/python/twitter/thermos:observer'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = [
    'sandbox_manager.py',
    'task_runner_wrapper.py',
  ],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/contextutil'),
    pants('src/python/twitter/common/http'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor.py',
  dependencies = [
    pants(':thermos_lib'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('3rdparty/python.twitter:mesos-latest')
  ]
)
