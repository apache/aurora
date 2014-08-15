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

from apache.aurora.client.api import UpdaterConfig

from gen.apache.aurora.api.ttypes import Range


class TestRangeConversion(unittest.TestCase):
  """Job instance ID to range conversion."""

  def test_multiple_ranges(self):
    """Test multiple ranges."""
    ranges = [repr(e) for e in UpdaterConfig.instances_to_ranges([1, 2, 3, 5, 7, 8])]
    assert 3 == len(ranges), "Wrong number of ranges:%s" % len(ranges)
    assert repr(Range(first=1, last=3)) in ranges, "Missing range [1,3]"
    assert repr(Range(first=5, last=5)) in ranges, "Missing range [5,5]"
    assert repr(Range(first=7, last=8)) in ranges, "Missing range [7,8]"

  def test_one_element(self):
    """Test one ID in the list."""
    ranges = [repr(e) for e in UpdaterConfig.instances_to_ranges([1])]
    assert 1 == len(ranges), "Wrong number of ranges:%s" % len(ranges)
    assert repr(Range(first=1, last=1)) in ranges, "Missing range [1,1]"

  def test_none_list(self):
    """Test None list produces None result."""
    assert UpdaterConfig.instances_to_ranges(None) is None, "Result must be None."

  def test_empty_list(self):
    """Test empty list produces None result."""
    assert UpdaterConfig.instances_to_ranges([]) is None, "Result must be None."
