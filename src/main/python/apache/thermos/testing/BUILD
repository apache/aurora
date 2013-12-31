python_library(
  name = 'runner',
  sources = ['runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/config'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos:thrift'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)
