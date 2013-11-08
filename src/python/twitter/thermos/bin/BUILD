python_binary(
  name = 'thermos_ckpt',
  source = 'thermos_ckpt.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/common'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_binary(
  name = 'thermos',
  source = 'thermos.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/common'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/core'),
    pants('src/python/twitter/thermos/monitoring'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ],
)
