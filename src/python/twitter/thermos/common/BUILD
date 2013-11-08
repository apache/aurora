python_library(
  name = 'ckpt',
  sources = ['ckpt.py'],
  dependencies = [
    pants('src/python/twitter/common/log'),
    pants('src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/thrift/com/twitter/thermos:py-thrift')
  ]
)

python_library(
  name = 'path',
  sources = ['path.py'],
)

python_library(
  name = 'planner',
  sources = ['planner.py'],
)

python_library(
  name = 'options',
  sources = ['options.py'],
  dependencies = [
    pants('src/python/twitter/thermos:pystachio'),
    pants('src/thrift/com/twitter/thermos:py-thrift'),
  ]
)

python_library(
  name = 'common',
  dependencies = [
    pants(':ckpt'),
    pants(':options'),
    pants(':path'),
    pants(':planner'),
  ],
  provides = setup_py(
    name = 'twitter.thermos.common',
    version = '0.1.0',
    description = 'Thermos common libraries.',
  )
)
