python_library(
  name = 'runner',
  sources = ['runner.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/contextutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/python/twitter/thermos/config'),
    pants('src/python/twitter/thermos/core'),
    pants('src/python/twitter/thermos:thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)
