python_library(
  name = 'detector',
  sources = ['detector.py'],
  dependencies = [
    pants('src/python/twitter/thermos/base')
  ]
)

python_library(
  name = 'garbage',
  sources = ['garbage.py'],
  dependencies = [
    pants('src/python/twitter/common/dirutil'),
    pants('src/python/twitter/thermos/base'),
    pants(':detector'),
  ]
)

python_library(
  name = 'measure',
  sources = ['measure.py'],
  dependencies = [
    pants('3rdparty/python:psutil'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/quantity'),
  ]
)

python_library(
  name = 'monitor',
  sources = ['monitor.py'],
  dependencies = [
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'muxer',
  sources = ['muxer.py'],
  dependencies = [
    pants('src/python/twitter/common/lang'),
    pants(':monitor')
  ]
)

python_library(
  name = 'monitoring',
  dependencies = [
    pants(':detector'),
    pants(':garbage'),
    pants(':measure'),
    pants(':monitor'),
    pants(':muxer')
  ]
)
