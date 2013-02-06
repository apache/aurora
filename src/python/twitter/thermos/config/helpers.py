"""Helpers for composing Thermos workflows."""

import itertools

from twitter.common.lang import Compatibility

from .schema import (
   Constraint,
   Process,
   Resources,
   Task,
)

from pystachio import Empty, List


__all__ = (
  # shorthand for process ordering constraint
  'order',

  # task combinators
  'combine_tasks',    # merge N tasks in parallel
  'concat_tasks',     # serialize N tasks

  # options helpers
  'java_options',
  'python_options',

  # the automatically-sequential version of a task
  'SequentialTask',

  # helper classes
  'Options',
  'Processes',
  'Tasks',
  'Units',
)


class Units(object):
  """Helpers for base units of Tasks and Processes."""

  @classmethod
  def optional_resources(cls, resources):
    return Resources() if resources is Empty else resources

  @classmethod
  def resources_sum(cls, *resources):
    """Add two Resources objects together."""
    def add_unit(f1, f2):
      return (0 if f1 is Empty else f1.get()) + (0 if f2 is Empty else f2.get())

    def add(r1, r2):
      return Resources(cpu=add_unit(r1.cpu(), r2.cpu()),
                       ram=add_unit(r1.ram(), r2.ram()),
                       disk=add_unit(r1.disk(), r2.disk()))

    return reduce(add, map(cls.optional_resources, resources), Resources(cpu=0, ram=0, disk=0))

  @classmethod
  def resources_max(cls, resources):
    """Return a Resource object that is the maximum of the inputs along each
      resource dimension."""
    def max_unit(f1, f2):
      return max(0 if f1 is Empty else f1.get(), 0 if f2 is Empty else f2.get())

    def resource_max(r1, r2):
      return Resources(cpu=max_unit(r1.cpu(), r2.cpu()),
                       ram=max_unit(r1.ram(), r2.ram()),
                       disk=max_unit(r1.disk(), r2.disk()))

    return reduce(resource_max,
        map(cls.optional_resources, resources), Resources(cpu=0, ram=0, disk=0))

  @classmethod
  def processes_merge(cls, tasks):
    """Return a deduped list of the processes from all tasks."""
    return list(set(itertools.chain.from_iterable(task.processes() for task in tasks)))

  @classmethod
  def constraints_merge(cls, tasks):
    """Return a deduped list of the constraints from all tasks."""
    return list(set(itertools.chain.from_iterable(task.constraints() for task in tasks)))


class Processes(object):
  """Helper class for Process objects."""

  @classmethod
  def _process_name(cls, process):
    if isinstance(process, Process):
      return process.name()
    elif isinstance(process, Compatibility.string):
      return process
    raise ValueError("Unknown value for process order: %s" % repr(process))

  @classmethod
  def order(cls, *processes):
    """Given a list of processes, return the list of constraints that keeps them in order, e.g.
       order(p1, p2, p3) => [Constraint(order=[p1.name(), p2.name(), p3.name()])].

       Similarly, concatenation operations are valid, i.e.
          order(p1, p2) + order(p2, p3) <=> order(p1, p2, p3)
    """
    return [Constraint(order=[cls._process_name(p) for p in processes])]


class Tasks(object):
  """Helper class for Task objects."""

  @classmethod
  def _combine_processes(cls, *tasks):
    """Given multiple tasks, merge their processes together, retaining the identity of the first
       task."""
    if len(tasks) == 0:
      return Task()
    head_task = tasks[0]
    return head_task(processes=Units.processes_merge(tasks))

  @classmethod
  def combine(cls, *tasks, **kw):
    """Given multiple tasks, return a Task that runs all processes in parallel."""
    if len(tasks) == 0:
      return Task()
    base = cls._combine_processes(*tasks)
    return base(
      resources=Units.resources_sum(*(task.resources() for task in tasks)),
      constraints=Units.constraints_merge(tasks),
      **kw
    )

  @classmethod
  def concat(cls, *tasks, **kw):
    """Given tasks T1...TN, return a single Task that runs all processes such that
       all processes in Tk run before any process in Tk+1."""
    if len(tasks) == 0:
      return Task()
    base = cls._combine_processes(*tasks)
    base = base(resources=Units.resources_max(task.resources() for task in tasks))
    base_constraints = Units.constraints_merge(tasks)
    for (t1, t2) in zip(tasks[0:-1], tasks[1:]):
      for p1 in t1.processes():
        for p2 in t2.processes():
          if p1 != p2:
            base_constraints.extend(Processes.order(p1, p2))
    return base(constraints=base_constraints, **kw)

  @classmethod
  def sequential(cls, task):
    """Add a constraint that makes all processes within a task run sequentially."""
    def maybe_constrain(task):
      return {'constraints': order(*task.processes())} if task.processes() is not Empty else {}
    if task.constraints() is Empty or task.constraints() == List(Constraint)([]):
      return task(**maybe_constrain(task))
    raise ValueError('Cannot turn a Task with existing constraints into a SequentialTask!')


class Options(object):
  """Helper class for constructing command-line arguments."""

  @classmethod
  def render_option(cls, short_prefix, long_prefix, option, value=None):
    option = '%s%s' % (short_prefix if len(option) == 1 else long_prefix, option)
    return '%s %s' % (option, value) if value else option

  @classmethod
  def render_options(cls, short_prefix, long_prefix, *options, **kw_options):
    renders = []

    for option in options:
      if isinstance(option, Compatibility.string):
        renders.append(cls.render_option(short_prefix, long_prefix, option))
      elif isinstance(option, dict):
        # preserve order in case option is an OrderedDict, rather than recursing with **option
        for argument, value in option.items():
          renders.append(cls.render_option(short_prefix, long_prefix, argument, value))
      else:
        raise ValueError('Got an unexpected argument to render_options: %s' % repr(option))

    for argument, value in kw_options.items():
      renders.append(cls.render_option(short_prefix, long_prefix, argument, value))

    return renders

  @classmethod
  def java(cls, *options, **kw_options):
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
    return ' '.join(cls.render_options('-', '-', *options, **kw_options))

  @classmethod
  def python(cls, *options, **kw_options):
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
    return ' '.join(cls.render_options('-', '--', *options, **kw_options))


def SequentialTask(*args, **kw):
  """A Task whose processes are always sequential."""
  return Tasks.sequential(Task(*args, **kw))


python_options = Options.python
java_options = Options.java


combine_tasks = Tasks.combine
concat_tasks = Tasks.concat


order = Processes.order
