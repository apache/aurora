python_library(
  name = 'client',
  sources = globs('*.py'),
  dependencies = [
    python_requirement('mesos==0.9.0'),
    python_requirement('thrift'),
    python_requirement('tornado'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/zookeeper'),
    pants('src/python/twitter/mesos/executor:sandbox_manager'),
    pants('src/python/twitter/mesos/executor:task_runner_wrapper'),
    pants('src/python/twitter/mesos/executor:thermos_executor_lib'),
    pants('src/python/twitter/mesos/packer'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/mesos:location'),
    pants('src/python/twitter/mesos:parsers'),
    pants('src/python/twitter/mesos:scheduler_client'),
    pants('src/python/twitter/mesos:session_key_helper'),
    pants('src/python/twitter/mesos:updater'),
    pants('src/python/twitter/thermos/observer'),
    pants('src/thrift/com/twitter/common/zookeeper/testing/angrybird:py-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

