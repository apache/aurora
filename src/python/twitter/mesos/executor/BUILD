python_library(
  name = 'sandbox_manager',
  sources = ['sandbox_manager.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/appapp')
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
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/python/twitter/thermos/monitoring:monitor'),
    pants('src/python/twitter/thermos/config:schema'),
  ]
)

python_library(
  name = 'executor_detector',
  sources = ['executor_detector.py'],
  dependencies = [
    pants('src/python/twitter/common/string'),
    pants('src/python/twitter/mesos:clusters'),
  ]
)

python_library(
  name = 'http_signaler',
  sources = ['http_signaler.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
  ]
)

python_library(
  name = 'health_interface',
  sources = ['health_interface.py'],
  dependencies = [python_requirement('mesos')]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    pants(':health_interface'),
    pants('src/python/twitter/common/exceptions')
  ]
)

python_library(
  name = 'kill_manager',
  sources = ['kill_manager.py'],
  dependencies = [
    pants(':health_interface'),
  ]
)

python_library(
  name = 'discovery_manager',
  sources = ['discovery_manager.py'],
  dependencies = [
    pants(':health_interface'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants('src/python/twitter/common_internal/zookeeper:twitter_service'),
  ]
)

python_library(
  name = 'resource_manager',
  sources = ['resource_manager.py'],
  dependencies = [
    pants(':health_interface'),
    pants(':resource_checkpoints'),
    python_requirement('psutil>=0.5.0'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/monitoring:disk'),
    pants('src/python/twitter/thermos/monitoring:resource'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'resource_checkpoints',
  sources = ['resource_checkpoints.py'],
  dependencies = [
    pants(':executor_detector'),
    python_requirement('thrift'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'health_interfaces',
  dependencies = [
    pants(':health_checker'),
    pants(':kill_manager'),
    pants(':resource_manager'),
    pants(':discovery_manager'),
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
    pants(':thermos_executor_base'),
    pants(':health_interfaces'),
    python_requirement('mesos'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
    python_requirement('mesos'),
  ]
)

python_library(
  name = 'thermos_executor_lib',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants(':http_signaler'),
    pants(':resource_manager'),
    pants(':status_manager'),
    pants(':task_runner_wrapper'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/concurrent'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/mesos:parsers'), # TODO(wickman) Fix this messy dependency
  ]
)

python_binary(
  name = 'thermos_executor',
  source = 'thermos_executor_main.py',
  ignore_errors = True,
  dependencies = [
    pants(':thermos_executor_lib')
  ]
)

python_binary(
  name = 'thermos_runner',
  source = 'thermos_runner.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/runner'),
  ],
)

python_library(
  name = 'gc_executor_lib',
  sources = ['gc_executor.py', 'gc_executor_main.py'],
  dependencies = [
    pants(':executor_detector'),
    pants(':sandbox_manager'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/common/concurrent'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/process'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/python/twitter/thermos/runner:inspector'),
    pants('src/python/twitter/thermos/monitoring:detector'),
    pants('src/python/twitter/thermos/monitoring:garbage'),
  ]
)

python_binary(
  name = 'gc_executor',
  entry_point = 'twitter.mesos.executor.gc_executor_main',
  ignore_errors = True,
  dependencies = [
    pants(':gc_executor_lib'),
    pants('src/python/twitter/common/app'),
  ]
)
