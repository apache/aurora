# Alias for src/python/twitter/aurora/config/schema
python_library(
  name = 'schema',
  dependencies = [
    pants('src/python/twitter/aurora/config/schema'),
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
    pants('src/python/twitter/aurora/BUILD.thirdparty:pystachio'),
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('src/python/twitter/aurora/common'),
    pants('src/python/twitter/aurora/config/schema'),
    pants('src/thrift/com/twitter/aurora/gen:py-thrift'),
  ],
  provides = setup_py(
    name = 'twitter.aurora.config',
    version = '0.1.0',
    description = 'Aurora/Thermos Pystachio schemas for describing job configurations.',
    license = 'Apache License, Version 2.0',
  )
)
