python_library(
  name = 'helper',
  sources = ['helper.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/process'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/base'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'inspector',
  sources = ['inspector.py'],
  dependencies = [
    pants(':muxer'),
    pants('src/python/twitter/thermos/base:ckpt'),
    pants('src/python/twitter/thermos/base:path')
  ]
)

python_library(
  name = 'muxer',
  sources = ['muxer.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'process',
  sources = ['process.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'runner',
  sources = ['__init__.py', 'runner.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('3rdparty/python:thrift-0.7'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/common/process'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
    pants(':helper'),
    pants(':muxer'),
    pants(':process'),
  ]
)
