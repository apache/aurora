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


  max_failures  = Default(Integer, 1) # maximum number of failed process runs
                                      # before process is failed.
  daemon        = Default(Integer, 0) # boolean
  ephemeral     = Default(Integer, 0) # boolean
  min_duration  = Default(Integer, 5) # integer seconds


@Provided(thermos = ThermosContext)
class Task(Struct):
  name = Required(String)
  processes = List(Process)

  # optionals
  constraints = List(Constraint)
  resources = Resources
  max_failures = Default(Integer, 1) # maximum number of failed processes before task is failed.
  max_concurrency = Default(Integer, 0) # 0 = infinite concurrency, > 0 => max concurrent processes.
  user = Default(String, '{{thermos.user}}')
