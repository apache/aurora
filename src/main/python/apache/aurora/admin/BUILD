python_library(
  name = 'mesos_maintenance',
  sources = 'mesos_maintenance.py',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/twitter/aurora/client:api'),
    pants('src/main/python/twitter/aurora/client:base'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
