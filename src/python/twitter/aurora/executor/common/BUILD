python_library(
  name = 'health_interface',
  sources = ['health_interface.py'],
  dependencies = [
    pants('src/python/twitter/common/metrics'),
  ]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    pants(':health_interface'),
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
    pants('src/python/twitter/aurora/executor/common:health_interface'),
  ]
)

python_library(
  name = 'sandbox',
  sources = ['sandbox.py'],
  dependencies = [
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/appapp')
  ]
)
