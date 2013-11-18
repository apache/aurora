python_library(
  name = 'thermos_task_runner',
  sources = ['thermos_task_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/common'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/config:schema'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/monitoring:monitor'),
    pants('src/python/twitter/aurora/common:http_signaler'),
    pants('src/python/twitter/aurora/executor/common:status_checker'),
    pants('src/python/twitter/aurora/executor/common:task_info'),
    pants('src/python/twitter/aurora/executor/common:task_runner'),
  ]
)

python_library(
  name = 'executor_detector',
  sources = ['executor_detector.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'executor_vars',
  sources = ['executor_vars.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('aurora/twitterdeps/src/python/twitter/common/python'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/string'),
  ]
)

python_library(
  name = 'status_manager',
  sources = ['status_manager.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/python/twitter/aurora/executor/common:status_checker'),
  ]
)

python_library(
  name = 'thermos_executor_base',
  sources = ['executor_base.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/thrift/com/twitter/thermos:py-thrift'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'thermos_executor',
  sources = ['thermos_executor.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':status_manager'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/python/twitter/aurora/executor/common:kill_manager'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora/executor/common:task_info'),
    pants('src/python/twitter/aurora/executor/common:task_runner'),
    pants('src/python/twitter/aurora:mesos-core'),
  ]
)

python_library(
  name = 'thermos_runner',
  sources = ['thermos_runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/common:planner'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/config:schema'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core'),
  ],
)

python_library(
  name = 'gc_executor',
  sources = ['gc_executor.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants(':executor_detector'),
    pants(':thermos_executor_base'),
    pants('aurora/twitterdeps/src/python/twitter/common/collections'),
    pants('aurora/twitterdeps/src/python/twitter/common/exceptions'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/metrics'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/common:ckpt'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/common:path'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core:helper'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/core:inspector'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/monitoring:detector'),
    pants('aurora/twitterdeps/src/python/twitter/thermos/monitoring:garbage'),
    pants('src/python/twitter/aurora/config:schema'),
    pants('src/python/twitter/aurora/executor/common:sandbox'),
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
