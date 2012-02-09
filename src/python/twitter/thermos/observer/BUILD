OBSERVER_ASSETS = rglobs('http/assets/*') + rglobs('http/templates/*')
python_library(
  name = 'observer',
  sources = rglobs('*.py'),
  resources = OBSERVER_ASSETS,
  dependencies = [
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/common/lang'),
    pants('src/python/twitter/common/quantity'),
    pants('3rdparty/python:bottle'),
    pants('3rdparty/python:mako'),
    pants('3rdparty/python:tornado'),
  ]
)
