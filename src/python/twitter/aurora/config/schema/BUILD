python_library(
  name = 'schema',
  sources = ['base.py'],
  dependencies = [
    pants('src/python/twitter/aurora/BUILD.thirdparty:pystachio'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
