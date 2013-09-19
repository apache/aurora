python_library(
  name = 'config',
  sources = (
    '__init__.py',
    'loader.py',
    'port_resolver.py',
    'thrift.py',
  ),
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/aurora/common'),
    pants('src/python/twitter/aurora/config/schema'),
    pants('src/python/twitter/common/lang'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

# Alias for src/python/twitter/aurora/config/schema
python_library(
  name = 'schema',
  dependencies = [
    pants('src/python/twitter/aurora/config/schema'),
  ]
)