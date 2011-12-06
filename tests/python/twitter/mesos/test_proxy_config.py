import exceptions
import pytest
import unittest
from twitter.mesos.proxy_config import ProxyMesosConfig

def test_parsing_constraints():
  # Test parse primitive value type
  constraints_dict = {
    'int': '2',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == 'int'
  assert c.constraint.valueConstraint.negated == False
  assert c.constraint.valueConstraint.value.intValue == 2

  constraints_dict = {
    'float': '2.0',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == 'float'
  assert c.constraint.valueConstraint.negated == False
  assert c.constraint.valueConstraint.value.doubleValue == 2.0

  constraints_dict = {
    'str': '2.0a',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == 'str'
  assert c.constraint.valueConstraint.negated == False
  assert c.constraint.valueConstraint.value.stringValue == '2.0a'

  # Test negate value
  constraints_dict = {
    '!str': '!2.0a',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == '!str'
  assert c.constraint.valueConstraint.negated == True
  assert c.constraint.valueConstraint.value.stringValue == '2.0a'

  # Test list
  constraints_dict = {
    'list': '1,2,3,a,b,c',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == 'list'
  assert c.constraint.listConstraint.negated == False
  assert c.constraint.listConstraint.values == ['1', '2', '3', 'a', 'b', 'c']

  constraints_dict = {
    '!list': '!1,2,3,a,b,c',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == '!list'
  assert c.constraint.listConstraint.negated == True
  assert c.constraint.listConstraint.values == ['1', '2', '3', 'a', 'b', 'c']

  # Test limit
  constraints_dict = {
    'limit': 'limit:4',
  }
  c = ProxyMesosConfig.parse_constraints(constraints_dict).pop()
  assert c.attribute == 'limit'
  assert c.constraint.limitConstraint.limit == 4

  constraints_dict = {
    'limit': 'limit:a',
  }
  with pytest.raises(exceptions.ValueError):
    constraints = ProxyMesosConfig.parse_constraints(constraints_dict)
