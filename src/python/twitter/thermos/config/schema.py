from twitter.common.lang import Compatibility

from pystachio import (
  Default,
  Empty,
  Float,
  Integer,
  List,
  Map,
  Provided,
  Required,
  String,
  Struct
)


# Define constants for resources
BYTES = 1
KB = 1024 * BYTES
MB = 1024 * KB
GB = 1024 * MB
TB = 1024 * GB


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
  final         = Default(Integer, 0) # if this process should be a finalizing process
                                      # that should always be run after regular processes


@Provided(thermos = ThermosContext)
class Task(Struct):
  name = Default(String, '{{processes[0].name}}')
  processes = List(Process)

  # optionals
  constraints = List(Constraint)
  resources = Resources
  max_failures = Default(Integer, 1) # maximum number of failed processes before task is failed.
  max_concurrency = Default(Integer, 0) # 0 = infinite concurrency, > 0 => max concurrent processes.
  finalization_wait = Default(Integer, 30) # the amount of time in seconds we allocate to run the
                                           # finalization schedule.
  user = Default(String, '{{thermos.user}}')



## Schema helpers

def process_name(process):
  if isinstance(process, Process):
    return process.name()
  elif isinstance(process, Compatibility.string):
    return process
  raise ValueError("Unknown value for process order: %s" % repr(process))

def order(*processes):
  """Given a list of processes, return the list of constraints that keeps them in order, e.g.
     order(p1, p2, p3) => [Constraint(order=[p1.name(), p2.name(), p3.name()])].

     Similarly, concatenation operations are valid, i.e.
        order(p1, p2) + order(p2, p3) <=> order(p1, p2, p3)
  """
  return [Constraint(order=[process_name(p) for p in processes])]


def add_resources(r1, r2):
  """Add two Resources objects together."""
  def add(f1, f2):
    return (0 if f1 is Empty else f1.get()) + (
            0 if f2 is Empty else f2.get())

  return Resources(cpu=add(r1.cpu(), r2.cpu()),
                   ram=add(r1.ram(), r2.ram()),
                   disk=add(r1.disk(), r2.disk()))


def combine_tasks(*tasks):
  """Given multiple tasks, merge their processes together, retaining the identity of the first
     task."""

  if len(tasks) == 0:
    return Task()

  processes = []
  resources = Resources()
  constraints = []

  for task in tasks:
    processes.extend(task.processes())
    resources = add_resources(resources, task.resources())
    constraints.extend(task.constraints())

  head_task = tasks[0]
  return head_task(processes=processes, resources=resources, constraints=constraints)


def __render_option(short_prefix, long_prefix, option, value=None):
  option = '%s%s' % (short_prefix if len(option) == 1 else long_prefix, option)
  return '%s %s' % (option, value) if value else option


def __render_options(short_prefix, long_prefix, *options, **kw_options):
  renders = []

  for option in options:
    if isinstance(option, Compatibility.string):
      renders.append(__render_option(short_prefix, long_prefix, option))
    elif isinstance(option, dict):
      # preserve order in case option is an OrderedDict, rather than recursing with **option
      for argument, value in option.items():
        renders.append(__render_option(short_prefix, long_prefix, argument, value))
    else:
      raise ValueError('Got an unexpected argument to render_options: %s' % repr(option))

  for argument, value in kw_options.items():
    renders.append(__render_option(short_prefix, long_prefix, argument, value))

  return renders


def java_options(*options, **kw_options):
  """
    Given a set of arguments, keyword arguments or dictionaries, render
    command-line parameters accordingly.  For example:

      java_options('a', 'b') == '-a -b'
      java_options({
        'a': 23,
        'b': 'foo'
      }) == '-a 23 -b foo'
      java_options(a=23, b='foo') == '-a 23 -b foo'
  """
  return ' '.join(__render_options('-', '-', *options, **kw_options))


def python_options(*options, **kw_options):
  """
    Given a set of arguments, keyword arguments or dictionaries, render
    command-line parameters accordingly.  Single letter parameters are
    rendered with single '-'.  For example:

      python_options('a', 'boo') == '-a --boo'
      python_options({
        'a': 23,
        'boo': 'foo'
      }) == '-a 23 --boo foo'
      python_options(a=23, boo='foo') == '-a 23 --boo foo'
  """
  return ' '.join(__render_options('-', '--', *options, **kw_options))
