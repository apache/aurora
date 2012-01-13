python_library(
  name = "schema",
  sources = ['schema.py'],
  dependencies = [
    pants("src/python/twitter/thermos/config:schema")
  ]
)

python_binary(
  name = "repl",
  source = "mesos_repl.py",
  dependencies = [
    pants(":schema")
  ]
)
