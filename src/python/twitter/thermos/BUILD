python_library(
  name = 'base',
  sources = rglobs('base/*.py'),
  dependencies = [
    pants('src/python/twitter/common'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_library(
  name = 'monitoring',
  sources = rglobs('monitoring/*.py'),
  dependencies = []
)

OBSERVER_ASSETS = rglobs('observer/http/assets/*')
OBSERVER_TEMPLATES = rglobs('observer/http/templates/*')
python_library(
  name = 'observer',
  sources = rglobs('observer/**.py'),
  resources = OBSERVER_ASSETS + OBSERVER_TEMPLATES,
  dependencies = [
    pants(':base'),
    pants(':monitoring'),
    pants('src/python/twitter/common'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/tcl:py-thrift'),
    pants('3rdparty/python:mako'),
    pants('3rdparty/python:bottle')
  ]
)

python_library(
  name = 'runner',
  sources = rglobs('runner/*.py'),
  dependencies = [
    pants(':base'),
    pants('src/python/twitter/common'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/tcl:scheduler'),
    pants('src/thrift/com/twitter/tcl:py-thrift')
  ]
)
