#
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest

import pytest

from apache.aurora.config.thrift import constraints_to_thrift


def test_parsing_constraints():
  constraints_dict = {
    'int': '2',
  }
  c = constraints_to_thrift(constraints_dict).pop()
  assert c.name == 'int'
  assert c.constraint.value.negated == False
  assert c.constraint.value.values == set(['2'])

  # Test negated value
  constraints_dict = {
    '!str': '!foo',
  }
  c = constraints_to_thrift(constraints_dict).pop()
  assert c.name == '!str'
  assert c.constraint.value.negated == True
  assert c.constraint.value.values == set(['foo'])

  # Test list
  constraints_dict = {
    'set': '1,2,3,a,b,c',
  }
  c = constraints_to_thrift(constraints_dict).pop()
  assert c.name == 'set'
  assert c.constraint.value.negated == False
  assert c.constraint.value.values == set(['1', '2', '3', 'a', 'b', 'c'])

  constraints_dict = {
    '!set': '!1,2,3,a,b,c',
  }
  c = constraints_to_thrift(constraints_dict).pop()
  assert c.name == '!set'
  assert c.constraint.value.negated == True
  assert c.constraint.value.values == set(['1', '2', '3', 'a', 'b', 'c'])

  # Test limit
  constraints_dict = {
    'limit': 'limit:4',
  }
  c = constraints_to_thrift(constraints_dict).pop()
  assert c.name == 'limit'
  assert c.constraint.limit.limit == 4

  constraints_dict = {
    'limit': 'limit:a',
  }
  with pytest.raises(ValueError):
    constraints = constraints_to_thrift(constraints_dict)
