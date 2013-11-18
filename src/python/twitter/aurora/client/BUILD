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
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'binding_helper',
  sources = ['binding_helper.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
  ]
)

python_library(
  name = 'config',
  sources = ['config.py'],
  dependencies = [
    pants('3rdparty/python:pystachio'),
    pants(':base'),
    pants(':binding_helper'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/python/twitter/aurora/config'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'factory',
  sources = ['factory.py'],
  dependencies = [
    pants(':base'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('src/python/twitter/aurora/client/hooks'),
    pants('src/python/twitter/aurora/common:cluster'),
    pants('src/python/twitter/aurora/common:clusters'),
  ]
)


python_library(
  name = 'options',
  sources = ['options.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/thermos/common:options'),
    pants('src/python/twitter/aurora/common:aurora_job_key'),
  ]
)
