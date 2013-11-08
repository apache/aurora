page(name = 'readme', source = 'README.md')

python_library(
  name = 'thermos',
  dependencies = [
    pants('src/python/twitter/thermos/core'),
  ],
  provides = setup_py(
    name = 'twitter.thermos',
    version = '0.1.0',
    descripton = 'The Twitter Thermos runtime.',
  ).with_binaries(
    thermos = pants('src/python/twitter/thermos/bin:thermos'),
  )
)
