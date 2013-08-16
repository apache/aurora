
from abc import abstractmethod

from twitter.common.lang import Interface

# The registry for binding helpers.
BINDING_HELPERS = None


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
  def get_ref_kind(self):
    """Returns a name describing the kind of refs that this helper can bind.
    This is used as an index for binding dictionaries, and as such, it should be
    unique.
    """

  @abstractmethod
  def get_ref_pattern(self):
    """Returns a pystachio pattern for refs that can be bound by this helper."""


def register_binding_helper(helper):
  BINDING_HELPERS.append(helper)


def register_binding_helpers():
  global BINDING_HELPERS
  BINDING_HELPERS = []
  from twitter.mesos.client.binding_helpers.packer_helper import PackerBindingHelper
  from twitter.mesos.client.binding_helpers.jenkins_helper import JenkinsBindingHelper
  register_binding_helper(PackerBindingHelper())
  register_binding_helper(JenkinsBindingHelper())


def apply_binding_helpers(config, env=None, force_local=False, binding_dict=None):
  """Computes a set of bindings and applies them to the config.

  :param config: the config whose bindings need to be computed.
  :param binding_dict: an optional dictionary containing data to be used to compute the
      bindings. If this is provided, then data from the dictionary should be used in
      preference over live data.
  :return: a binding dictionary with data that can be used to recompute the bindings. The
      config is updated in-place.
  """
  if BINDING_HELPERS is None:
    register_binding_helpers()

  for helper in BINDING_HELPERS:
    ref_kind = helper.get_ref_kind()
    if ref_kind not in config.binding_dicts:
      config.binding_dicts[ref_kind] = {}
    binding_dict = config.binding_dicts[ref_kind]
    for match in helper.get_ref_pattern().match(config.raw()):
      helper.bind_ref(config, match, env, force_local, binding_dict)
