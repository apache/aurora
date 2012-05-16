from pystachio import (
  Default,
  Float,
  Integer,
  List,
  Map,
  Provided,
  Required,
  String,
  Struct
)


class ThermosContext(Struct):
  ports   = Map(String, Integer)
  task_id = String
  user    = String


# TODO(wickman)  Resource management/enforcement currently unsupported by Thermos
# and it's questionable whether it will ever be enforced or available in a standard
# way.  It could feasibly be enforced through some plugin mechanism perhaps.
class Resources(Struct):
  cpu  = Required(Float)
  ram  = Required(Integer)
  disk = Required(Integer)


class Constraint(Struct):
  order = List(String)


class Process(Struct):
  cmdline = Required(String)
  name    = Required(String)

  # optionals
  resources     = Resources
  daemon        = Default(Integer, 0)   # boolean, currently unsupported
  max_failures  = Default(Integer, 1)


@Provided(thermos = ThermosContext)
class Task(Struct):
  name      = Required(String)
  resources = Resources
  processes = List(Process)
  constraints = List(Constraint)

  max_failures  = Default(Integer, 1)

  # 0 = infinite concurrency, > 0 => max concurrent processes.
  max_concurrency = Default(Integer, 0)

  user          = Default(String, '{{thermos.user}}')
