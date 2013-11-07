python_library(
  name = 'common',
  sources = ['__init__.py'],
  dependencies = [
    pants(':aurora_job_key'),
    pants(':cluster'),
    pants(':cluster_option'),
    pants(':clusters'),
    pants(':http_signaler'),
  ]
)

python_library(
  name = 'aurora_job_key',
  sources = ['aurora_job_key.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'cluster',
  sources = ['cluster.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
  ]
)

python_library(
  name = 'clusters',
  sources = ['clusters.py'],
  dependencies = [
    pants(':cluster'),
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/common/collections'),
  ]
)

python_library(
  name = 'cluster_option',
  sources = ['cluster_option.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
  ]
)

python_library(
  name = 'http_signaler',
  sources = ['http_signaler.py'],
  dependencies = [
    pants('3rdparty/python:socksipy-branch'),  # TODO(wickman) MESOS-3710
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
  ]
)
