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

from apache.aurora.config.resource import ResourceDetails, ResourceManager, ResourceType

from gen.apache.aurora.api.ttypes import Resource, ResourceAggregate, TaskConfig


class TestResourceType(unittest.TestCase):
  def test_from_resource(self):
    assert ResourceType.from_resource(Resource(numCpus=1.0)) is ResourceType.CPUS
    assert ResourceType.from_resource(Resource(ramMb=1)) is ResourceType.RAM_MB
    assert ResourceType.from_resource(Resource(diskMb=0)) is ResourceType.DISK_MB
    assert ResourceType.from_resource(Resource(namedPort='http')) is ResourceType.PORTS

  def test_resource_value(self):
    assert ResourceType.CPUS.resource_value(Resource(numCpus=1.0)) == 1.0


class TestResourceManager(unittest.TestCase):
  def test_resource_details(self):
    details = ResourceManager.resource_details([Resource(ramMb=2), Resource(numCpus=1.0)])
    assert len(details) == 2
    assert details[0] == ResourceDetails(ResourceType.CPUS, 1.0)
    assert details[1] == ResourceDetails(ResourceType.RAM_MB, 2)

  def test_quantity_of(self):
    quantity = ResourceManager.quantity_of(
        ResourceManager.resource_details([Resource(ramMb=2), Resource(numCpus=1.0)]),
        ResourceType.CPUS)
    assert quantity == 1.0

  def test_backfill_quota(self):
    quota = ResourceAggregate(numCpus=1.0, ramMb=2, diskMb=3)
    assert ResourceManager.resource_details_from_quota(quota) == [
        ResourceDetails(ResourceType.CPUS, 1.0),
        ResourceDetails(ResourceType.RAM_MB, 2),
        ResourceDetails(ResourceType.DISK_MB, 3)
    ]

  def test_backfill_task(self):
    task = TaskConfig(numCpus=1.0, ramMb=2, diskMb=3, requestedPorts=frozenset(['http']))
    assert ResourceManager.resource_details_from_quota(task) == [
        ResourceDetails(ResourceType.CPUS, 1.0),
        ResourceDetails(ResourceType.RAM_MB, 2),
        ResourceDetails(ResourceType.DISK_MB, 3),
        ResourceDetails(ResourceType.PORTS, 'http'),
    ]
