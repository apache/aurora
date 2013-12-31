python_binary(
  name = 'thermos_ckpt',
  source = 'thermos_ckpt.py',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/thrift/org/apache/thermos:py-thrift')
  ]
)

python_binary(
  name = 'thermos',
  source = 'thermos.py',
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos/monitoring'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ],
)
