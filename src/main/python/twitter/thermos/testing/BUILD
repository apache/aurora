python_library(
  name = 'runner',
  sources = ['runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/twitter/thermos/common:ckpt'),
    pants('src/main/python/twitter/thermos/common:path'),
    pants('src/main/python/twitter/thermos/config'),
    pants('src/main/python/twitter/thermos/core'),
    pants('src/main/python/twitter/thermos:thrift'),
    pants('src/main/thrift/com/twitter/thermos:py-thrift'),
  ]
)
