python_binary(
  name = "mesos_client",
  source = "mesos_client.py",
  dependencies = [
    pants("src/python/twitter/mesos/mesos:mesos-lib"),
    pants("src/python/twitter/common")
  ]
)

python_binary(
  name = "deploy_scheduler",
  source = "deploy_scheduler.py",
  dependencies = [
    pants("src/python/twitter/mesos/mesos:mesos-lib"),
    pants("src/python/twitter/common"),
    pants("3rdparty/python:git-python")
  ]
)
