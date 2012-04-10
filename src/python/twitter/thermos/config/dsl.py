from pystachio import Empty
from twitter.thermos.config.schema import (
  ProcessConstraint,
  ProcessPair,
)


class ConstraintFunctions(object):
  @staticmethod
  def before(p1, p2):
    return ProcessConstraint(ordered = [ProcessPair(first = p1, second = p2)])


class Constraint(object):
  _FUNCTIONS = {
    'before': ConstraintFunctions.before,
    'after': lambda p1, p2: ConstraintFunctions.before(p2, p1)
  }

  class UnknownConstraintError(Exception):
    pass

  @staticmethod
  def delegate(function, p1, p2):
    assert function in Constraint._FUNCTIONS, "Unknown constraint %s" % function
    return Constraint._FUNCTIONS[function](p1, p2)

  @staticmethod
  def parse(constraint_expr):
    split_expr = constraint_expr.strip().split()
    if len(split_expr) != 3:
      raise Constraint.UnknownConstraintError("Unknown constraint: %s" % constraint_expr)
    p1, verb, p2 = split_expr
    return Constraint.delegate(verb, p1, p2)


def with_schedule(task, *constraints):
  new_constraints = []
  for constraint in constraints:
    new_constraints.append(Constraint.parse(constraint))
  existing_constraints = task.constraints() if task.has_constraints() else []
  return task(constraints = existing_constraints + new_constraints)


def in_order(processes):
  constraints = []
  for k in range(1, len(processes)):
    constraints.append(ProcessPair(first = processes[k-1].name(), second = processes[k].name()))
  return [ProcessConstraint(ordered = constraints)]
