python_binary(
  name = 'thermos_ckpt',
  source = 'thermos_ckpt.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/thermos:base'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_binary(
  name = 'thermos_run',
  source = 'thermos_run.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/tcl:loader'),
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:runner'),
    pants('src/python/twitter/common/metrics')
  ]
)

python_binary(
  name = 'thermos_observe',
  source = 'thermos_observe.py',
  dependencies = [
    pants('src/python/twitter/tcl:loader'),
    pants('src/python/twitter/thermos:base'),
    pants('src/python/twitter/thermos:observer'),

    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/metrics'),
    pants('src/python/twitter/common/metrics:vars'),
    pants('src/python/twitter/common/http'),
  ]
)
