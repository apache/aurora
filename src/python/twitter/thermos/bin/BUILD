python_binary(
  name = 'thermos_ckpt',
  source = 'thermos_ckpt.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/base'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_binary(
  name = 'thermos_run',
  source = 'thermos_run.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:exceptions'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/runner'),
  ]
)

python_binary(
  name = 'thermos',
  source = 'thermos.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/monitoring:detector'),
    pants('src/python/twitter/thermos/monitoring:garbage'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_binary(
  name = 'thermos_observe',
  source = 'thermos_observe.py',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/app/modules:all'),
    pants('src/python/twitter/thermos/observer'),
    pants('src/python/twitter/thermos/monitoring'),
  ]
)
