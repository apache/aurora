python_library(
  name = 'base',
  sources = rglobs('base/*.py'),
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_library(
  name = 'monitoring',
  sources = rglobs('monitoring/*.py'),
  dependencies = []
)

OBSERVER_ASSETS = rglobs('observer/assets/*')
python_library(
  name = 'observer',
  sources = rglobs('observer/*.py'),
  resources = OBSERVER_ASSETS,
  dependencies = [
    pants(':base'),
    pants(':monitoring'),
    pants('src/thrift/com/twitter/tcl:py-thrift')
  ]
)

python_library(
  name = 'runner',
  sources = rglobs('runner/*.py'),
  dependencies = [
    pants(':base'),
    pants('src/python/twitter/tcl:scheduler'),
    pants('src/thrift/com/twitter/tcl:py-thrift')
  ]
)
