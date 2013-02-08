python_library(
  name = 'deploy',
  sources = globs('*.py'),
  dependencies = [
    pants('src/python/twitter/common/git'),
    pants('src/python/twitter/mesos:clusters'),
    pants('src/python/twitter/vert:base'),
    pants('src/python/twitter/vert:backends')
  ]
)
