import exceptions
import pytest
import unittest
from twitter.mesos.parsers.mesos_config import MesosConfig

def test_parsing_constraints():
  constraints_dict = {
    'int': '2',
  }
  c = MesosConfig.constraints_to_thrift(constraints_dict).pop()
  assert c.attribute == 'int'
  assert c.constraint.value.negated == False
  assert c.constraint.value.values == set(['2'])

  # Test negated value
  constraints_dict = {
    '!str': '!foo',
  }
  c = MesosConfig.constraints_to_thrift(constraints_dict).pop()
  assert c.attribute == '!str'
  assert c.constraint.value.negated == True
  assert c.constraint.value.values == set(['foo'])

  # Test list
  constraints_dict = {
    'set': '1,2,3,a,b,c',
  }
  c = MesosConfig.constraints_to_thrift(constraints_dict).pop()
  assert c.attribute == 'set'
  assert c.constraint.value.negated == False
  assert c.constraint.value.values == set(['1', '2', '3', 'a', 'b', 'c'])

  constraints_dict = {
    '!set': '!1,2,3,a,b,c',
  }
  c = MesosConfig.constraints_to_thrift(constraints_dict).pop()
  assert c.attribute == '!set'
  assert c.constraint.value.negated == True
  assert c.constraint.value.values == set(['1', '2', '3', 'a', 'b', 'c'])

  # Test limit
  constraints_dict = {
    'limit': 'limit:4',
  }
  c = MesosConfig.constraints_to_thrift(constraints_dict).pop()
  assert c.attribute == 'limit'
  assert c.constraint.limitConstraint.limit == 4

  constraints_dict = {
    'limit': 'limit:a',
  }
  with pytest.raises(exceptions.ValueError):
    constraints = MesosConfig.constraints_to_thrift(constraints_dict)
