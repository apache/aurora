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
  name = 'kill_manager',
  sources = ['kill_manager.py'],
  dependencies = [
    pants('src/python/twitter/aurora/executor/common:health_interface'),
  ]
)

python_library(
  name = 'discovery_manager',
  sources = ['discovery_manager.py'],
  dependencies = [
    pants('src/python/twitter/aurora/executor/common:health_interface'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/zookeeper/serverset:kazoo_serverset'),
    pants('src/python/twitter/common_internal/zookeeper:twitter_service_kazoo'),
  ]
)

python_library(
  name = 'resource_manager',
  sources = ['resource_manager.py'],
  dependencies = [
    pants('src/python/twitter/aurora/executor/common:health_interface'),
    pants(':resource_checkpoints'),
    pants('3rdparty/python:psutil'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/monitoring:disk'),
    pants('src/python/twitter/thermos/monitoring:resource'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
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
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'health_interfaces',
  dependencies = [
    pants('src/python/twitter/aurora/executor/common:health_checker'),
    pants(':kill_manager'),
    pants(':resource_manager'),
    pants(':discovery_manager'),
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    pants(':health_interfaces'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':executor_detector'),
    pants(':kill_manager'),
    pants(':resource_manager'),
    pants(':status_manager'),
    pants(':task_runner_wrapper'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/aurora/common:http_signaler'),
    pants('src/python/twitter/aurora/common_internal:clusters'),
    pants('src/python/twitter/aurora/config'),
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/concurrent'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_binary(
  name = 'thermos_runner',
  source = 'thermos_runner.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/app/modules:chickadee_handler'),
    pants('src/python/twitter/thermos/base:planner'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/runner'),
  ],
)

python_library(
  name = 'gc_executor',
  sources = ['gc_executor.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants(':executor_detector'),
    pants(':sandbox_manager'),
    pants(':thermos_executor_base'),
    pants('src/python/twitter/aurora/common_internal:clusters'),
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/python/twitter/common/collections'),
    pants('src/python/twitter/common/concurrent'),
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/base:ckpt'),
    pants('src/python/twitter/thermos/base:path'),
    pants('src/python/twitter/thermos/monitoring:detector'),
    pants('src/python/twitter/thermos/monitoring:garbage'),
    pants('src/python/twitter/thermos/runner:helper'),
    pants('src/python/twitter/thermos/runner:inspector'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
