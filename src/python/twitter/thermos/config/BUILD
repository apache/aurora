python_library(
  name = "schema",
  sources = globs('*.py'),
  dependencies = [
    pants("3rdparty/python:pystachio"),
    pants("src/python/twitter/common/lang"),
    pants("src/python/twitter/thermos/base:planner")
  ]
)

python_binary(
  name = "repl",
  source = "bin/config_repl.py",
  dependencies = [pants(":schema")]
)

python_binary(
  name = "load",
  source = "bin/config_load.py",
  dependencies = [
    pants(":schema"),
    pants("src/python/twitter/common/app")
  ]
)
