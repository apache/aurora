python_library(
  name = "mesos-lib",
  sources = rglobs("*.py"),
  dependencies = [
    pants('3rdparty/python:zookeeper'),
    pants('3rdparty/python:python-ldap'),
    pants('src/thrift/com/twitter/thrift:py-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/python/twitter/common')
  ]
)
