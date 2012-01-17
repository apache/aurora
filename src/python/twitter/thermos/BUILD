python_library(
  name = 'base',
  sources = rglobs('base/*.py'),
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

OBSERVER_ASSETS = rglobs('observer/http/assets/*') + rglobs('observer/http/templates/*')
python_library(
  name = 'observer',
  sources = rglobs('observer/*.py'),
  resources = OBSERVER_ASSETS,
  dependencies = [
    pants(':base'),
    pants('src/python/twitter/common/process'),
    pants('src/thrift/com/twitter/tcl:py-thrift'),
    pants('3rdparty/python:bottle'),
    pants('3rdparty/python:mako')
  ]
)

python_library(
  name = 'runner',
  sources = rglobs('runner/*.py'),
  dependencies = [
    pants(':base'),
    pants('src/python/twitter/common/process'),
    pants('src/python/twitter/common/quantity'),
    pants('src/python/twitter/thermos/config:schema')
  ]
)
