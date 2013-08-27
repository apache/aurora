python_library(
  name = 'mesos_maintenance',
  sources = 'mesos_maintenance.py',
  dependencies = [
    pants('src/python/twitter/aurora/client:api'),
    pants('src/python/twitter/aurora/client:base'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/servermaint:batching'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
