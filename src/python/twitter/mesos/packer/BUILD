python_library(
  name = 'packer',
  sources = globs('*.py'),
  dependencies = [
      pants('3rdparty/python:poster'),
      pants('src/python/twitter/common/log'),
      pants('src/python/twitter/common/net:ssh'),
      pants('src/python/twitter/common/zookeeper'),
      pants('src/python/twitter/common/zookeeper/group'),
      # TODO(wfarner): Remove.
      pants('src/thrift/com/twitter/thrift:py-thrift'),
      pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)