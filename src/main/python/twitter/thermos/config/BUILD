import os

python_library(
  name = 'schema',
  sources = globs('*.py'),
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('src/main/python/twitter/thermos:pystachio'),
    pants('src/main/python/twitter/thermos/common:planner')
  ],
)

python_library(
  name = 'config',
  dependencies = [
    pants(':schema'),
    pants('src/main/python/twitter/thermos/common'),  # cover common:planner
  ],
  provides = setup_py(
    name = 'twitter.thermos.config',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().lower(),
    description = 'Thermos configuration schema and loader.',
  )
)
