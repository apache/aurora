python_library(
  name = 'helper',
  sources = ['helper.py'],
  dependencies = [
    pants('src/python/twitter/thermos:psutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/dirutil'),
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/quantity'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'inspector',
  sources = ['inspector.py'],
  dependencies = [
    pants(':muxer'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'muxer',
  sources = ['muxer.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
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
    pants('src/thrift/com/twitter/thermos:py-thrift'),
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
    pants('src/python/twitter/thermos:psutil'),
    pants('src/python/twitter/thermos/common:ckpt'),
    pants('src/python/twitter/thermos/common:path'),
    pants('src/python/twitter/thermos/common:planner'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'core',
  dependencies = [
    pants(':inspector'),
    pants(':runner'),

    # covering libs
    pants('src/python/twitter/thermos/common'),
    pants('src/python/twitter/thermos/config'),
  ],
  provides = setup_py(
    name = 'twitter.thermos.core',
    version = '0.1.0',
    description = 'The Thermos core state machine.',
  )
)
