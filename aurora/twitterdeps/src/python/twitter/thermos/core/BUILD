python_requirement(
  name = 'core',
  requirement = 'twitter.thermos.core==0.1.0',
)

# sub-targets in thermos core tree
python_library(
  name = 'helper',
  dependencies = [
    pants(':core'),
  ]
)

python_library(
  name = 'inspector',
  dependencies = [
    pants(':core'),
  ]
)

python_library(
  name = 'muxer',
  dependencies = [
    pants(':core'),
  ]
)

python_library(
  name = 'process',
  dependencies = [
    pants(':core'),
  ]
)

python_library(
  name = 'runner',
  dependencies = [
    pants(':core'),
  ]
)
