import re

from gen.twitter.mesos.ttypes import (
  Constraint,
  LimitConstraint,
  TaskConstraint,
  ValueConstraint
)
from twitter.common.lang import Compatibility


class ThriftCodec(object):
  @staticmethod
  def constraints_to_thrift(constraints):
    """Convert a python dictionary to a set of Constraint thrift objects."""
    result = set()
    for attribute, constraint_value in constraints.items():
      assert isinstance(attribute, Compatibility.string) and (
             isinstance(constraint_value, Compatibility.string)), (
        "Both attribute name and value in constraints must be string")
      constraint = Constraint()
      constraint.name = attribute
      task_constraint = TaskConstraint()
      if constraint_value.startswith('limit:'):
        task_constraint.limitConstraint = LimitConstraint()
        try:
          task_constraint.limitConstraint.limit = int(constraint_value.replace('limit:', '', 1))
        except ValueError:
          print('%s is not a valid limit value, must be integer' % constraint_value)
          raise
      else:
        # Strip off the leading negation if present.
        negated = constraint_value.startswith('!')
        if negated:
          constraint_value = constraint_value[1:]
        task_constraint.value = ValueConstraint(negated, set(constraint_value.split(',')))
      constraint.constraint = task_constraint
      result.add(constraint)
    return result


class EntityParser(object):
  PORT_RE = re.compile(r'%port:(\w+)%')
  SHARD_ID = '%shard_id%'
  TASK_ID = '%task_id%'

  @staticmethod
  def match_ports(str):
    return set(EntityParser.PORT_RE.findall(str))
