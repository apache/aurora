python_binary(
  name = 'thermos',
  source = 'bin/thermos.py',
  dependencies = [
    pants(':thermos-lib'),
  ],
)

python_library(
  name = 'thermos-lib',
  dependencies = [
    pants('src/python/twitter/common/app'),
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/thermos/base'),
    pants('src/python/twitter/thermos/config:schema'),
    pants('src/python/twitter/thermos/monitoring'),
    pants('src/python/twitter/thermos/runner'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ],
)


page(name = 'readme', source = 'README.md')
