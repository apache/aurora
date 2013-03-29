python_library(
  name = 'config',
  sources = ['__init__.py', 'thrift.py'],
  dependencies = [
    pants(':schema'),
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/common/lang'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_library(
  name = 'schema',
  sources = globs('*.py') - ['__init__.py', 'thrift.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/vert:schema'),
    pants('src/thrift/com/twitter/mesos/gen:py-thrift'),
  ]
)

python_binary(
  name = 'repl',
  source = 'repl.py',
  dependencies = [
    pants(':schema')
  ]
)
