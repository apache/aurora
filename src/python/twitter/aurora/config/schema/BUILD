python_library(
  name = 'schema',
  sources = ['base.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
