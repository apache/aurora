import os

python_library(
  name = 'ckpt',
  sources = ['ckpt.py'],
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/log'),
    pants('aurora/twitterdeps/src/python/twitter/common/recordio:recordio-thrift'),
    pants('src/main/thrift/com/twitter/thermos:py-thrift')
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
    pants('src/main/python/twitter/thermos:pystachio'),
    pants('src/main/thrift/com/twitter/thermos:py-thrift'),
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
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().lower(),
    description = 'Thermos common libraries.',
  )
)
