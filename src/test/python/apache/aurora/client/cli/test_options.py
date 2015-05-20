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
from argparse import ArgumentTypeError

import pytest

from apache.aurora.client.cli.options import parse_instances


class TestParseInstances(unittest.TestCase):

  def test_parse_instances_with_range(self):
    assert parse_instances("0-3") == [0, 1, 2, 3]

  def test_parse_instances_mixed(self):
    assert parse_instances("4,1-2,0-0") == [0, 1, 2, 4]

  def test_parse_instances_invalid_ranges(self):
    with pytest.raises(ArgumentTypeError):
      parse_instances("4-1")

    with pytest.raises(ArgumentTypeError):
      parse_instances("1-0")
