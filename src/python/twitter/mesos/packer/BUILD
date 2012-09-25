python_library(
  name = 'packer',
  sources = globs('*.py'),
  dependencies = [
    pants('3rdparty/python:poster'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/net:ssh'),
    pants('src/python/twitter/mesos:session_key_helper'),
    pants('src/python/twitter/mesos:zookeeper_helper'),
  ]
)
