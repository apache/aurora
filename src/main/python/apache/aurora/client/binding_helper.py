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

import inspect
import os
import sys
from abc import abstractmethod, abstractproperty

from twitter.common.lang import Interface

__all__ = (
  'BindingHelper',
  'CachingBindingHelper',
  'apply_all',
  'clear_binding_caches',
  'unregister_all',
)


# The registry for binding helpers.
_BINDING_HELPERS = []


# TODO(wickman) Update the pydocs to remove references to common_internal components.
class BindingHelper(Interface):
  """A component which resolves some set of pseudo-bindings in a config.

  Many bindings are too complex to resolve with bindings using the standard mechanisms,
  because they require some python computation to determine how to bind them. For example,
  for references like {{packer[role][pkg][version]}}, we need to talk to the packer to figure
  out the correct packer call for the desired cluster.

  A BindingHelper is responsible for resolving one of these types of pseudo-bindings.
  PackerBindingHelper will resolve "packer" bindings; BuildBindingHelper will resolve "build"
  bindings, JenkinsBindingHelper will resolve "jenkins" bindings, etc.

  A BindingHelper can be registered by calling "BindingHelper.register(Helper)". Instead of
  explicitly calling "inject" methods in populate_namespaces, it will compute the set of open
  bindings, and then call the appropriate helpers for each.

  The bindings can be computed either from scratch, or from a binding dictionary. A binding
  dictionary can be computed from live data, and then passed over an RPC connection, so that
  the bindings can be recomputed on the server.

  Each helper is responsible for computing its own binding dict. The data in the dict should
  meet two requirements: it should be enough data to allow it to produce exactly the same
  result as the scratch binding, and the data should provide information that makes the
  binding comprehensible for a human debugging a job.

  For example, a packer helper's binding dict should provide enough information to identify
  the HDFS file that should be used, but also the version number of the binary in packer,
  (because a human reader wants to know the version of the package, not the meaningless
  HDFS URL.
  """
  @classmethod
  def register(cls):
    _BINDING_HELPERS.append(cls())

  def apply(self, config, env=None, binding_dict=None):
    for match in self.matcher.match(config.raw()):
      self.bind(config, match, env, binding_dict or config.binding_dicts[self.name])

  @abstractproperty
  def name(self):
    """Returns the name of this BindingHelper.  Typically it is the first component of
       the matcher, e.g. if the matcher matches {{git[sha]}}, return "git"."""

  @abstractproperty
  def matcher(self):
    """Returns the pystachio matcher for refs that this binding helper binds."""

  @abstractmethod
  def bind(self, config, match, env, binding_dict):
    """Resolves a ref, adding a binding to the config."""


class CachingBindingHelper(BindingHelper):
  """A binding helper implementation that caches binding results"""
  def __init__(self):
    self.cache = {}

  def flush_cache(self):
    self.cache = {}

  def bind(self, config, match, env, binding_dict):
    if match not in self.cache:
      self.cache[match] = self.uncached_bind(config, match, env, binding_dict)
    config.bind(self.cache[match])

  @abstractmethod
  def uncached_bind(self, config, match, env, binding_dict):
    """Compute the binding for a ref that hasn't been seen before."""


def unregister_all():
  _BINDING_HELPERS[:] = []


def apply_all(config, env=None, binding_dict=None):
  """Computes a set of bindings and applies them to the config.

  :param config: the config whose bindings need to be computed.
  :param env: the python environment where the configuration was evaluated.
  :param binding_dict: an optional dictionary containing data to be used to compute the
      bindings. If this is provided, then data from the dictionary should be used in
      preference over live data.
  :return: a binding dictionary with data that can be used to recompute the bindings. The
      config is updated in-place.
  """
  for helper in _BINDING_HELPERS:
    helper.apply(config, env, binding_dict or config.binding_dicts[helper.name])


def clear_binding_caches():
  """Clear the binding helper's caches for testing."""
  for helper in _BINDING_HELPERS:
    if isinstance(helper, CachingBindingHelper):
      helper.flush_cache()
