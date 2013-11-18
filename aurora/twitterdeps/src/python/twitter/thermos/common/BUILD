python_requirement(
  name = 'common',
  requirement = 'twitter.thermos.common==0.1.0',
)

# sub-targets in the thermos tree
python_library(
  name = 'ckpt',
  dependencies = [
    pants(':common'),
  ]
)

python_library(
  name = 'options',
  dependencies = [
    pants(':common'),
  ]
)

python_library(
  name = 'path',
  dependencies = [
    pants(':common'),
  ]
)

python_library(
  name = 'planner',
  dependencies = [
    pants(':common'),
  ]
)
