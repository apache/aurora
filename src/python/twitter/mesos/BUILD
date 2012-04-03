python_library(
  name = "mesos-lib",
  sources = rglobs("*.py"),
  dependencies = [
    pants('3rdparty/python:zookeeper'),
    pants('3rdparty/python:paramiko'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common_internal/ods'),
    pants('src/python/twitter/mesos/config:schema'),
    pants('src/thrift/com/twitter/thrift:py-thrift'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_binary(
  name = "mesos_client",
  source = "bin/mesos_client.py",
  dependencies = [
    pants(":mesos-lib"),
    pants("src/python/twitter/common"),
    pants("src/python/twitter/thermos/base:options")
  ],
  platforms = [
    'linux-x86_64',
    'macosx-10.4-x86_64'
  ],
)

python_binary(
  name = "deploy_scheduler",
  source = "bin/deploy_scheduler.py",
  dependencies = [
    pants('3rdparty/python:git-python'),
    pants(":mesos-lib"),
    pants("src/python/twitter/common"),
  ]
)

python_binary(
  name = "deploy_thermos",
  source = "bin/deploy_thermos.py",
  dependencies = [
    pants(":mesos-lib"),
    pants("src/python/twitter/common"),
  ]
)


python_binary(
  name = "restart_job",
  source = "bin/restart_job.py",
  dependencies = [
    pants("src/python/twitter/common/log"),
    pants(":mesos-lib"),
    pants("3rdparty/python:git-python")
  ]
)

python_binary(
  name = "process_scraper",
  source = "process_scraper.py",
  dependencies = [
    pants("3rdparty/python:psutil"),
    pants("src/python/twitter/common/app"),
  ],
  platforms = [
    'linux-x86_64',
    'macosx-10.6-x86_64',
  ]
)
