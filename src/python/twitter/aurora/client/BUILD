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

python_library(
  name = 'factory',
  sources = ['factory.py'],
  dependencies = [
    pants(':base'),
    pants('src/python/twitter/aurora/client/hooks'),
    pants('src/python/twitter/aurora/common:cluster'),
    pants('src/python/twitter/aurora/common:clusters'),
    pants('src/python/twitter/common/app'),
  ]
)


python_library(
  name = 'options',
  sources = ['options.py'],
  dependencies = [
    pants('src/python/twitter/aurora/common:aurora_job_key'),
    pants('src/python/twitter/thermos/base:options'),
  ]
)
