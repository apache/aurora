python_library(
  name = 'packer',
  sources = globs('*.py'),
  dependencies = [
    pants('3rdparty/python:poster'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/common/zookeeper/serverset'),
    pants('src/python/twitter/common_internal/auth:ssh'),
    pants('src/python/twitter/common_internal/location'),
    pants('src/python/twitter/common_internal/zookeeper:tunneler'),
    # TODO(wickman) MESOS-3005 should split this out into a separate target
    pants('src/python/twitter/mesos/common_internal:clusters'),
  ]
)
