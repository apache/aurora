

python_library(
  name = "mesos-lib",
  sources = rglobs("*.py"),
  dependencies = [
    pants('3rdparty/python:thrift-0.7'),
    pants('3rdparty/python:zookeeper'),
    pants('3rdparty/python:python-ldap'),
    pants('3rdparty/python.twitter:mesos_thrift'),
    pants('3rdparty/python.twitter:endpoint'),
    pants('src/python/twitter/common')
  ]
)
