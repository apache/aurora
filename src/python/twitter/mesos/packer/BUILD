python_library(
  name = 'packer',
  sources = globs('*.py'),
  dependencies = [
    pants('3rdparty/python:poster'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants('src/python/twitter/common_internal/auth'),
    pants('src/python/twitter/common_internal/location'),
    pants('src/python/twitter/common_internal/zookeeper:tunneler'),
    pants('src/python/twitter/mesos:clusters'),
  ]
)
