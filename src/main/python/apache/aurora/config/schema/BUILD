python_library(
  name = 'schema',
  sources = ['base.py'],
  dependencies = [
    pants('src/main/python/apache/aurora/BUILD.thirdparty:pystachio'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)
