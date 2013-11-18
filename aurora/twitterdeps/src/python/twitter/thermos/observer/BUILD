python_requirement(
  name = 'observer',
  requirement = 'twitter.thermos.observer==0.1.0',
)

python_library(
  name = 'observed_task',
  dependencies = [
    pants(':observer'),
  ]
)

python_library(
  name = 'task_observer',
  dependencies = [
    pants(':observer'),
  ]
)
