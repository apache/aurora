# Alias for src/main/python/twitter/aurora/config/schema
python_library(
  name = 'schema',
  dependencies = [
    pants('src/main/python/twitter/aurora/config/schema'),
  ]
)

python_library(
  name = 'config',
  sources = (
    '__init__.py',
    'loader.py',
    'port_resolver.py',
    'thrift.py',
  ),
  dependencies = [
    pants('src/main/python/twitter/aurora/BUILD.thirdparty:pystachio'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('src/main/python/twitter/aurora/common'),
    pants('src/main/python/twitter/aurora/config/schema'),
    pants('src/main/thrift/com/twitter/aurora/gen:py-thrift'),
  ],

)

python_library(
  name = 'config-packaged',
  dependencies = [
    pants(':config'),

    # covering dependencies
    pants('src/main/python/twitter/thermos/config'),
  ],
  provides = setup_py(
    name = 'twitter.aurora.config',
    version = '0.1.2',
    description = 'Aurora/Thermos Pystachio schemas for describing job configurations.',
    license = 'Apache License, Version 2.0',
  )
)