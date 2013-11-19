python_library(
  name = 'schema',
  sources = ['base.py'],
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:pystachio'),
    pants('src/main/python/twitter/thermos/config:schema'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
