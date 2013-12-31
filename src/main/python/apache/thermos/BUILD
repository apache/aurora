import os

page(name = 'readme', source = 'README.md')

python_library(
  name = 'thermos',
  dependencies = [
    pants('src/main/python/apache/thermos/core'),
    pants('src/main/python/apache/thermos/monitoring'),
  ],
  provides = setup_py(
    name = 'apache.thermos',
    version = open(os.path.join(get_buildroot(), '.auroraversion')).read().strip().lower(),
    descripton = 'The Apache Aurora Thermos runtime.',
  ).with_binaries(
    thermos = pants('src/main/python/apache/thermos/bin:thermos'),
  )
)
