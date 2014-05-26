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

from apache.aurora.client.cli.bridge import Bridge, CommandProcessor


class CommandOne(CommandProcessor):
  def get_commands(self):
    return ['one', 'two', 'three']

  def execute(self, args):
    return 1


class CommandTwo(CommandProcessor):
  def get_commands(self):
    return ['three', 'four', 'five']

  def execute(self, args):
    return 2


class CommandThree(CommandProcessor):
  def get_commands(self):
    return ['six']

  def execute(self, args):
    return '3[%s]' % args[1]


class TestBridgedCommandLine(unittest.TestCase):
  def setUp(self):
    self.one = CommandOne()
    self.two = CommandTwo()
    self.three = CommandThree()

  def test_bridge_with_default_three(self):
    bridge = Bridge([self.one, self.two, self.three], default=self.three)
    assert bridge.execute(['test', 'one']) == 1
    assert bridge.execute(['test', 'two']) == 1
    assert bridge.execute(['test', 'three']) == 1
    assert bridge.execute(['test', 'four']) == 2
    assert bridge.execute(['test', 'five']) == 2
    assert bridge.execute(['test', 'six']) == '3[six]'
    assert bridge.execute(['test', 'seven']) == '3[seven]'
    assert bridge.execute(['test', 'eight']) == '3[eight]'

  def test_bridge_with_default_one(self):
    bridge = Bridge([self.one, self.two, self.three], default=self.one)
    assert bridge.execute(['test', 'one']) == 1
    assert bridge.execute(['test', 'two']) == 1
    assert bridge.execute(['test', 'three']) == 1
    assert bridge.execute(['test', 'four']) == 2
    assert bridge.execute(['test', 'five']) == 2
    assert bridge.execute(['test', 'six']) == '3[six]'
    assert bridge.execute(['test', 'seven']) == 1
    assert bridge.execute(['test', 'eight']) == 1

  def test_bridge_with_no_default(self):
    bridge = Bridge([self.one, self.two, self.three])
    assert bridge.execute(['test', 'one']) == 1
    assert bridge.execute(['test', 'two']) == 1
    assert bridge.execute(['test', 'three']) == 1
    assert bridge.execute(['test', 'four']) == 2
    assert bridge.execute(['test', 'five']) == 2
    assert bridge.execute(['test', 'six']) == '3[six]'
    self.assertRaises(SystemExit, bridge.execute, ['test', 'seven'])

  def test_bridge_ordering(self):
    bridge1 = Bridge([self.one, self.two, self.three])
    bridge2 = Bridge([self.two, self.one, self.three])
    assert bridge1.execute(['test', 'three']) == 1
    assert bridge2.execute(['test', 'three']) == 2
