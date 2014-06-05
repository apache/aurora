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

from copy import copy
from optparse import Option, OptionValueError


def _check_mesos_cluster(option, opt, value):
  cluster_name = value
  if option.clusters and cluster_name in option.clusters:
    return option.clusters[cluster_name]
  elif option.cluster_provider:
    return option.cluster_provider(cluster_name)

  cluster_list = ""
  if option.clusters:
    cluster_list = 'Valid options for clusters are %s' % ' '.join(option.clusters)

  raise OptionValueError(
      '%s is not a valid cluster for the %s option. %s' % (value, opt, cluster_list))


class ClusterOption(Option):
  """A command-line Option that requires a valid cluster name and returns a Cluster object.

  Use in an @app.command_option decorator to avoid boilerplate. For example:

    CLUSTER_PATH = os.path.expanduser('~/.clusters')
    CLUSTERS = Clusters.from_json(CLUSTER_PATH)

    @app.command
    @app.command_option(ClusterOption('--cluster', default='smf1-test', clusters=CLUSTERS))
    def get_health(args, options):
      if options.cluster.zk_server:
        do_something(options.cluster)

    @app.command
    @app.command_option(ClusterOption('-s',
      '--source_cluster',
      default='smf1-test',
      clusters=CLUSTERS,
      help='Source cluster to pull metadata from.'))
    @app.command_option(ClusterOption('-d',
      '--dest_cluster',
      clusters=CLUSTERS,
      default='smf1-test'))
    def copy_metadata(args, options):
      if not options.source_cluster:
        print('required option source_cluster missing!')
      metadata_copy(options.source_cluster, options.dest_cluster)
  """

  # Needed since we're creating a new type for validation - see optparse docs.
  TYPES = copy(Option.TYPES) + ('mesos_cluster',)
  TYPE_CHECKER = copy(Option.TYPE_CHECKER)
  TYPE_CHECKER['mesos_cluster'] = _check_mesos_cluster

  def __init__(self, *opt_str, **attrs):
    """
      *opt_str: Same meaning as in twitter.common.options.Option, at least one is required.
      **attrs: See twitter.common.options.Option, with the following caveats:

      Exactly one of the following must be provided:

      clusters: A static Clusters object from which to pick clusters.
      cluster_provider: A function that takes a cluster name and returns a Cluster object.
    """
    self.clusters = attrs.pop('clusters', None)
    self.cluster_provider = attrs.pop('cluster_provider', None)
    if not (self.clusters is not None) ^ (self.cluster_provider is not None):
      raise ValueError('Must specify exactly one of clusters and cluster_provider.')

    default_attrs = dict(
      default=None,
      action='store',
      type='mesos_cluster',
      help='Mesos cluster to use (Default: %%default)'
    )

    combined_attrs = default_attrs
    combined_attrs.update(attrs)  # Defensive copy
    Option.__init__(self, *opt_str, **combined_attrs)  # old-style superclass
