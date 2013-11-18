python_library(
  name = 'status_checker',
  sources = ['status_checker.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/metrics'),
  ]
)

python_library(
  name = 'task_runner',
  sources = ['task_runner.py'],
  dependencies = [
    pants(':status_checker'),
    pants('src/python/twitter/common/lang'),
  ]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    pants(':status_checker'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/exceptions'),
  ]
)

python_library(
  name = 'executor_timeout',
  sources = ['executor_timeout.py'],
  dependencies = [
    pants('src/python/twitter/common/exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_library(
  name = 'kill_manager',
  sources = ['kill_manager.py'],
  dependencies = [
    pants(':status_checker'),
  ]
)

python_library(
  name = 'sandbox',
  sources = ['sandbox.py'],
  dependencies = [
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
  ]
)

python_library(
  name = 'task_info',
  sources = ['task_info.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
    pants('src/python/twitter/aurora/config'),
    pants('src/python/twitter/aurora/config/schema'),
    pants('src/python/twitter/common/log'),
  ]
)
