python_library(
  name = 'thermos_task_runner',
  sources = ['thermos_task_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/config'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/monitoring'),
    pants('src/python/twitter/aurora/common:http_signaler'),
    pants('src/python/twitter/aurora/executor/common:status_checker'),
    pants('src/python/twitter/aurora/executor/common:task_info'),
    pants('src/python/twitter/aurora/executor/common:task_runner'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_library(
  name = 'executor_detector',
  sources = ['executor_detector.py'],
  dependencies = [
    pants('src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'executor_vars',
  sources = ['executor_vars.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/common/python'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'status_checkers',
  dependencies = [
    pants('src/python/twitter/aurora/executor/common:health_checker'),
    pants('src/python/twitter/aurora/executor/common:kill_manager'),
    pants('src/python/twitter/aurora/executor/common_internal:resource_manager'),
    pants('src/python/twitter/aurora/executor/common_internal:discovery_manager'),
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    pants(':status_checkers'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/thrift/com/twitter/thermos:py-thrift'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/thrift/com/twitter/thermos:py-thrift'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':executor_detector'),
    pants(':status_manager'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/aurora/common:http_signaler'),
    pants('src/python/twitter/aurora/executor/common:kill_manager'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor/common:task_info'),
    pants('src/python/twitter/aurora/executor/common:task_runner'),
    pants('src/python/twitter/aurora/executor/common_internal:resource_checkpoints'),
    pants('src/python/twitter/aurora/executor/common_internal:resource_manager'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_binary(
  name = 'thermos_runner',
  source = 'thermos_runner.py',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/config'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
  ],
)

python_library(
  name = 'gc_executor',
  sources = ['gc_executor.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants(':executor_detector'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/monitoring'),
    pants('src/python/twitter/aurora/common_internal:clusters'),
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor/common_internal:appapp_sandbox'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/collections'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
