import os

# Create an alias for the previous target
python_library(
  name = 'api',
  dependencies = [
    pants('src/main/python/apache/aurora/client/api')
  ]
)

python_library(
  name = 'base',
  sources = ['base.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
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
    pants('src/main/python/apache/aurora/BUILD.thirdparty:pystachio'),
    pants(':base'),
    pants(':binding_helper'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/thrift/org/apache/aurora/gen:py-thrift'),
  ]
)

python_library(
  name = 'factory',
  sources = ['factory.py'],
  dependencies = [
    pants(':base'),
    pants('aurora/twitterdeps/src/python/twitter/common/app'),
    pants('src/main/python/apache/aurora/client/hooks'),
    pants('src/main/python/apache/aurora/common:cluster'),
    pants('src/main/python/apache/aurora/common:clusters'),
  ]
)

python_library(
  name = 'options',
  sources = ['options.py'],
  dependencies = [
    pants('src/main/python/apache/thermos/common:options'),
    pants('src/main/python/apache/aurora/common:aurora_job_key'),
  ]
)

python_library(
  name = 'client-packaged',
  dependencies = [
    pants('src/main/python/apache/aurora/common'),
    pants('src/main/python/apache/aurora/config'),
    pants('src/main/python/apache/thermos/common'),
  ],
  provides = setup_py(
    name = 'apache.aurora.client',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
  ).with_binaries(
    aurora_admin = pants('src/main/python/apache/aurora/client/bin:aurora_admin'),
    aurora = pants('src/main/python/apache/aurora/client/bin:aurora_client'),
  )
)
