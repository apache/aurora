# Create an alias for the previous target
python_library(
  name = 'api',
  dependencies = [
    pants('src/python/twitter/aurora/client/api')
  ]
)

python_library(
  name = 'base',
  sources = ['base.py'],
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'config',
  sources = ['config.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':base'),
    pants('src/python/twitter/aurora/client/binding_helpers'),
    pants('src/python/twitter/aurora/config'),
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)
