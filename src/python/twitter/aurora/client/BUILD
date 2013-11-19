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
    pants('src/python/twitter/aurora/BUILD.thirdparty:pystachio'),
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

python_library(
  name = 'client-packaged',
  dependencies = [
    pants('src/python/twitter/aurora/common'),
    pants('src/python/twitter/aurora/config'),
  ],
  provides = setup_py(
    name = 'twitter.aurora.client',
    version = '0.1.0',
  ).with_binaries(
    aurora_admin = pants('src/python/twitter/aurora/client/bin:aurora_admin'),
    aurora_client = pants('src/python/twitter/aurora/client/bin:aurora_client'),
  )
)
