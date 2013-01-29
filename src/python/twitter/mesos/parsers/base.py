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
        task_constraint.limit = LimitConstraint()
        try:
          task_constraint.limit.limit = int(constraint_value.replace('limit:', '', 1))
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


class FormatDetector(object):
  THERMOS_CALLS = frozenset(('Job', 'Task', 'Process', 'include', 'export'))

  @classmethod
  def autodetect(cls, filename):
    import ast
    with open(filename, 'r') as fp:
      config = ast.parse(fp.read(), filename)
    def detect_call(node):
      if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        return node.func.id
    if set(filter(None, map(detect_call, ast.walk(config)))).intersection(cls.THERMOS_CALLS):
      return 'thermos'
    else:
      return 'mesos'


class PortResolver(object):
  class CycleException(Exception): pass

  @classmethod
  def resolve(cls, portmap):
    """
        Given an announce-style portmap, return a fully dereferenced portmap.

        For example, given the portmap:
          {
            'http': 80,
            'aurora: 'http',
            'https': 'aurora',
            'thrift': 'service'
          }

        Returns {'http': 80, 'aurora': 80, 'https': 80, 'thrift': 'service'}
    """
    for (name, port) in portmap.items():
      if not isinstance(name, Compatibility.string):
        raise ValueError('All portmap keys must be strings!')
      if not isinstance(port, (int, Compatibility.string)):
        raise ValueError('All portmap values must be strings or integers!')

    portmap = portmap.copy()
    for port in list(portmap):
      try:
        portmap[port] = int(portmap[port])
      except ValueError:
        continue

    def resolve_one(static_port):
      visited = set()
      root = portmap[static_port]
      while root in portmap:
        visited.add(root)
        if portmap[root] in visited:
          raise cls.CycleException('Found cycle in portmap!')
        root = portmap[root]
      return root

    return dict((name, resolve_one(name)) for name in portmap)

  @classmethod
  def unallocated(cls, portmap):
    """Given a resolved portmap, return the list of ports that need to be allocated."""
    return set(port for port in portmap.values() if not isinstance(port, int))

  @classmethod
  def bound(cls, portmap):
    """Given a resolved portmap, return the list of ports that have already been allocated."""
    return set(portmap)
