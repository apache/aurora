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
from collections import namedtuple
from enum import Enum, unique
from numbers import Number

from gen.apache.aurora.api.ttypes import Resource

ResourceDetails = namedtuple('ResourceDetails', ['resource_type', 'value'])


@unique
class ResourceType(Enum):
  """Describes Aurora resource types and their traits."""

  CPUS = ('numCpus', 'CPU', ' core(s)', float, 1)
  RAM_MB = ('ramMb', 'RAM', ' MB', int, 2)
  DISK_MB = ('diskMb', 'Disk', ' MB', int, 3)
  PORTS = ('namedPort', 'Port', '', str, 4)
  GPUS = ('numGpus', 'GPU', ' GPU(s)', int, 5)

  def __init__(self, field, display_name, display_unit, value_type, display_position):
    self._field = field
    self._display_name = display_name
    self._display_unit = display_unit
    self._value_type = value_type
    self._display_position = display_position

  @property
  def field(self):
    return self._field

  @property
  def display_name(self):
    return self._display_name

  @property
  def display_unit(self):
    return self._display_unit

  @property
  def value_type(self):
    return self._value_type

  @property
  def display_position(self):
    return self._display_position

  def resource_value(self, resource):
    return resource.__dict__.get(self._field)

  @classmethod
  def from_resource(cls, resource):
    for _, member in cls.__members__.items():
      if resource.__dict__.get(member.field) is not None:
        return member
    else:
      raise ValueError("Unknown resource: %s" % resource)


class ResourceManager(object):
  """Provides helper methods for working with Aurora resources."""

  @classmethod
  def resource_details(cls, resources):
    result = []
    if resources:
      for resource in list(resources):
        r_type = ResourceType.from_resource(resource)
        result.append(ResourceDetails(r_type, r_type.resource_value(resource)))
      return sorted(result, key=lambda rd: rd.resource_type.display_position)
    return result

  @classmethod
  def resource_details_from_quota(cls, quota):
    return cls.resource_details(quota.resources)

  @classmethod
  def resource_details_from_task(cls, task):
    return cls.resource_details(cls._backfill_resources(task))

  @classmethod
  def quantity_of(cls, resource_details, resource_type):
    result = 0.0
    for d in resource_details:
      if d.resource_type is resource_type:
        result += d.value if isinstance(d.value, Number) else 1
    return result

  @classmethod
  def _backfill_resources(cls, r_object):
    resources = list(r_object.resources) if r_object.resources else None
    if resources is None:
      resources = [
          Resource(numCpus=r_object.numCpus),
          Resource(ramMb=r_object.ramMb),
          Resource(diskMb=r_object.diskMb)
      ]
      if hasattr(r_object, 'requestedPorts'):
        resources += [Resource(namedPort=p) for p in r_object.requestedPorts or []]
    return resources
