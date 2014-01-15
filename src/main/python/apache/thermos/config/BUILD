import os

python_library(
  name = 'schema',
  sources = globs('*.py'),
  dependencies = [
    pants('aurora/twitterdeps/src/python/twitter/common/lang'),
    pants('src/main/python/apache/thermos:pystachio'),
    pants('src/main/python/apache/thermos/common:planner')
  ],
)

python_library(
  name = 'config',
  dependencies = [
    pants(':schema'),
    pants('src/main/python/apache/thermos/common'),  # cover common:planner
  ],
  provides = setup_py(
    name = 'apache.thermos.config',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().upper(),
    description = 'Thermos configuration schema and loader.',
  )
)
