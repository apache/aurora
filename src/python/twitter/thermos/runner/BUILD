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
  sources = ['__init__.py', 'planner.py', 'runner.py'],
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
    pants(':process'),
    pants(':muxer')
  ]
)
