python_requirement(
  name = 'monitoring',
  requirement = 'twitter.thermos.monitoring==0.1.0',
)


# sub-targets for thermos monitoring module
python_library(
  name = 'detector',
  dependencies = [
    pants(':monitoring'),
  ]
)

python_library(
  name = 'garbage',
  dependencies = [
    pants(':monitoring'),
  ]
)

python_library(
  name = 'monitor',
  dependencies = [
    pants(':monitoring'),
  ]
)

python_library(
  name = 'disk',
  dependencies = [
    pants(':monitoring'),
  ]
)

python_library(
  name = 'process',
  dependencies = [
    pants(':monitoring'),
  ]
)

python_library(
  name = 'process_collector_psutil',
  dependencies = [
    pants(':monitoring'),
  ]
)

python_library(
  name = 'resource',
  dependencies = [
    pants(':monitoring'),
  ]
)
