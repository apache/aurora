python_library(
  name = 'mesos_maintenance',
  sources = 'mesos_maintenance.py',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('src/main/python/apache/aurora/client:api'),
    pants('src/main/python/apache/aurora/client:base'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
