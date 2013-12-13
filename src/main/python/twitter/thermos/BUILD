import os

page(name = 'readme', source = 'README.md')

python_library(
  name = 'thermos',
  dependencies = [
    pants('src/main/python/twitter/thermos/core'),
    pants('src/main/python/twitter/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'twitter.thermos',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().lower(),
    descripton = 'The Twitter Thermos runtime.',
  ).with_binaries(
    thermos = pants('src/main/python/twitter/thermos/bin:thermos'),
  )
)
