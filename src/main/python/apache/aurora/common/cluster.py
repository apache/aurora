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

from pystachio import Empty, Struct
from pystachio.composite import Structural

__all__ = ('Cluster',)


# TODO(wickman)  It seems like some of this Trait/Mixin stuff should be a
# first-class construct in Pystachio.  It could be a solution for extensible
# Job/Task definitions.
class Cluster(dict):
  """Cluster encapsulates a set of K/V attributes describing cluster configurations.

  Given a cluster, attributes may be accessed directly on them, e.g.
    cluster.name
    cluster.scheduler_zk_path

  In order to enforce particular "traits" of Cluster, use Cluster.Trait to construct
  enforceable schemas, e.g.

    class ResolverTrait(Cluster.Trait):
      scheduler_zk_ensemble = Required(String)
      scheduler_zk_path = Default(String, '/twitter/service/mesos/prod/scheduler')

    cluster = Cluster(name = 'west', scheduler_zk_ensemble = 'zookeeper.west.twttr.net')

    # Ensures that scheduler_zk_ensemble is defined in the cluster or it will raise a TypeError
    cluster.with_trait(ResolverTrait).scheduler_zk_ensemble

    # Will use the default if none is provided on Cluster.
    cluster.with_trait(ResolverTrait).scheduler_zk_path
  """
  Trait = Struct

  def __init__(self, **kwargs):
    self._traits = ()
    super(Cluster, self).__init__(**kwargs)

  def get_trait(self, trait):
    """Given a Cluster.Trait, extract that trait."""
    if not issubclass(trait, Structural):
      raise TypeError('provided trait must be a Cluster.Trait subclass, got %s' % type(trait))
    # TODO(wickman) Expose this in pystachio as a non-private or add a load method with strict=
    return trait(trait._filter_against_schema(self))

  def check_trait(self, trait):
    """Given a Cluster.Trait, typecheck that trait."""
    trait_check = self.get_trait(trait).check()
    if not trait_check.ok():
      raise TypeError(trait_check.message())

  def with_traits(self, *traits):
    """Return a cluster annotated with a set of traits."""
    new_cluster = self.__class__(**self)
    for trait in traits:
      new_cluster.check_trait(trait)
    new_cluster._traits = traits
    return new_cluster

  def with_trait(self, trait):
    """Return a cluster annotated with a single trait (helper for self.with_traits)."""
    return self.with_traits(trait)

  def __setitem__(self, key, value):
    raise TypeError('Clusters are immutable.')

  def __getattr__(self, attribute):
    for trait in self._traits:
      expressed_trait = self.get_trait(trait)
      if hasattr(expressed_trait, attribute):
        value = getattr(expressed_trait, attribute)()
        return None if value is Empty else value.get()
    try:
      return self[attribute]
    except KeyError:
      return self.__getattribute__(attribute)

  def __copy__(self):
    return self

  def __deepcopy__(self, memo):
    return self
