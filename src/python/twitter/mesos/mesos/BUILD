python_library(
  name = "mesos-lib",
  sources = rglobs("*.py"),
  dependencies = [
    pants('3rdparty/python:zookeeper'),
    pants('3rdparty/python:paramiko'),
    pants('src/thrift/com/twitter/thrift:py-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
    pants('src/python/twitter/common')
  ]
)

python_binary(
  name = "mesos_configuration",
  dependencies = [ pants(":mesos-lib") ],
  source = "mesos_configuration.py"
)
