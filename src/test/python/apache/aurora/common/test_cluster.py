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

import pytest
from pystachio import Default, Integer, Required, String

from apache.aurora.common.cluster import Cluster


def test_simple():
  class AudubonTrait(Cluster.Trait):
    master_role = String
    slave_role  = Default(String, 'slave')
    version     = Required(Integer)

  west = Cluster(name = 'west',
                 master_role = 'west.master',
                 slave_role = 'west.slave',
                 version = 10)
  east = Cluster(name = 'east', version = 11)

  assert east.name == 'east'
  with pytest.raises(AttributeError):
    east.slave_role
  assert east.with_traits(AudubonTrait).slave_role == 'slave'
  assert west.with_traits(AudubonTrait).slave_role == 'west.slave'
  assert east.with_traits(AudubonTrait).master_role is None

  with pytest.raises(TypeError):
    # requires version at least
    Cluster(name = 'east').with_traits(AudubonTrait)
