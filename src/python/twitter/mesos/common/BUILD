python_library(
  name = 'http_signaler',
  sources = ['http_signaler.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net'),
    pants('src/python/twitter/common_internal/location'),
    pants('src/python/twitter/mesos:clusters'),
    python_requirement('SocksiPy-branch>=1.02')
  ]
)
