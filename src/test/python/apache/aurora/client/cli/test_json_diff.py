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

from apache.aurora.client.cli.json_tree_diff import (
    compare_json,
    compare_pruned_json,
    FieldDifference,
    prune_structure
)


class TestJsonDiff(unittest.TestCase):

  LIST_ONE = {
      "a": [1, 2, 3],
      "x": {
          "y": {
              "z": 3,
              "a": {
                  "c": 2,
                  "d": 3
              }
          },
          "z": {
              "a": 27,
              "b": "foo",
          },
          "q": "br",
      },
      "y": "foo",
      "z": "zoom",
      "q": "fizzboom"
  }

  LIST_TWO = {
      "a": [3, 2, 1],
      "x": {
          "y": {
              "z": 5,
              "a": {
                  "c": 2,
                  "d": 3
              }
          },
          "z": {
              "a": 27,
              "b": "foo"
          },
          "q": "bar",
      },
      "y": "foo",
      "z": "zoom",
  }

  def test_pruner(self):
    ex = [["x", "y", "z"], ["x", "y", "a"], ["x", "z"], "q"]
    assert prune_structure(self.LIST_ONE, ex) == {
        "a": [1, 2, 3],
        "x": {
            "y": {},
            "q": "br",
        },
        "y": "foo",
        "z": "zoom"
    }

  def test_compare_canonicalized(self):
    one = {"a": ["1", "2", ["3", "4"]]}
    two = {"a": ["2", ["3", "4"], "1"]}
    assert compare_json(one, two, []) == []

  def test_compare_json(self):
    result = compare_json(self.LIST_ONE, self.LIST_TWO, [])
    expected = [FieldDifference(name='q', base='fizzboom', other='__undefined__'),
        FieldDifference(name='x.q', base='br', other='bar'),
        FieldDifference(name='x.y.z', base=3, other=5)]
    assert result == expected

  def test_compare_pruned(self):
    assert compare_pruned_json(self.LIST_ONE, self.LIST_TWO, [['x', 'y']]) == [
        FieldDifference(name='q', base='fizzboom', other='__undefined__'),
        FieldDifference(name='x.q', base='br', other='bar')]

    assert compare_pruned_json(self.LIST_ONE, self.LIST_TWO, [['x', 'y'], ['x', 'q']]) == [
        FieldDifference(name='q', base='fizzboom', other='__undefined__')]

    assert compare_pruned_json(self.LIST_ONE, self.LIST_TWO, ['x', 'q']) == []
