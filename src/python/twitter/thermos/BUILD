python_binary(
  name = 'thermos',
  source = 'bin/thermos.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/monitoring:detector'),
    pants('src/python/twitter/thermos/monitoring:garbage'),
    pants('src/python/twitter/thermos/monitoring:monitor'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ],
  platforms = ['linux-x86_64'],
)
