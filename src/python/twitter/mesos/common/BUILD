python_library(
  name = 'common',
  sources = ['__init__.py'],
  dependencies = [
    pants(':aurora_job_key'),
    pants(':http_signaler'),
  ]
)

python_library(
  name = 'aurora_job_key',
  sources = ['aurora_job_key.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'http_signaler',
  sources = ['http_signaler.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net'),
    pants('src/python/twitter/common/net:socks'),
    pants('src/python/twitter/common_internal/location'),
    pants('src/python/twitter/mesos:clusters'),
  ]
)
