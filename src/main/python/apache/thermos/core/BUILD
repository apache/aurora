import os

python_library(
  name = 'helper',
  sources = ['helper.py'],
  dependencies = [
    pants('src/main/python/apache/thermos:psutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'inspector',
  sources = ['inspector.py'],
  dependencies = [
    pants(':muxer'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'muxer',
  sources = ['muxer.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'process',
  sources = ['process.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'runner',
  sources = ['__init__.py', 'runner.py'],
  dependencies = [
    pants(':helper'),
    pants(':muxer'),
    pants(':process'),
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/python/apache/thermos:psutil'),
    pants('src/main/python/apache/thermos/common:ckpt'),
    pants('src/main/python/apache/thermos/common:path'),
    pants('src/main/python/apache/thermos/common:planner'),
    pants('src/main/python/apache/thermos/config:schema'),
    pants('src/main/thrift/org/apache/thermos:py-thrift'),
  ]
)

python_library(
  name = 'core',
  dependencies = [
    pants(':inspector'),
    pants(':runner'),

    # covering libs
    pants('src/main/python/apache/thermos/common'),
    pants('src/main/python/apache/thermos/config'),
  ],
  provides = setup_py(
    name = 'apache.thermos.core',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'The Thermos core state machine.',
  )
)
