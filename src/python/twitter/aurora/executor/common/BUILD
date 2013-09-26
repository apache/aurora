python_library(
  name = 'health_interface',
  sources = ['health_interface.py'],
  dependencies = [
    pants('src/python/twitter/aurora:mesos-core'),
    pants('src/python/twitter/common/metrics'),
  ]
)

python_library(
  name = 'health_checker',
  sources = ['health_checker.py'],
  dependencies = [
    pants(':health_interface'),
    pants('src/python/twitter/common/exceptions')
  ]
)
