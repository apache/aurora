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

from apache.aurora.client.cli.options import (
    binding_parser,
    instance_specifier,
    parse_instances,
    parse_task_instance_key
)


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

  def test_instance_specifier_fails_on_empty(self):
    with pytest.raises(ArgumentTypeError):
      instance_specifier("")

  def test_parse_task_instance_key_missing_instance(self):
    with pytest.raises(ArgumentTypeError):
      parse_task_instance_key("cluster/role/env/name")

  def test_parse_task_instance_key_fails_on_range(self):
    with pytest.raises(ArgumentTypeError):
      parse_task_instance_key("cluster/role/env/name/0-5")

  def binding_parser_fails_on_invalid_parts(self):
    with pytest.raises(ArgumentTypeError):
      binding_parser("p=")

  def binding_parser_fails_parsing(self):
    with pytest.raises(ArgumentTypeError):
      binding_parser("p=2342")
