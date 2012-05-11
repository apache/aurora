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
    pants('src/python/twitter/thermos/monitoring'),
    python_requirement('bottle'),
    python_requirement('tornado'),
    python_requirement('Mako==0.7.0'),
  ]
)
