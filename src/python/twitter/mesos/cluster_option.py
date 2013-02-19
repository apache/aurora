from copy import copy

from twitter.common.lang import Compatibility
from twitter.common.options import Option, OptionValueError

from .clusters import Cluster

def _check_mesos_cluster(option, opt, value):
  """Type-checker callback for mesos_cluster option type (see optparse docs)."""
  option_value_error = OptionValueError(
    '%s is not a valid cluster for %s option. Valid clusters are %s.' % (
      value, opt, option.valid_clusters))

  try:
    cluster = Cluster.get(value)
  except Cluster.UnknownCluster:
    raise option_value_error

  if not option.cluster_filter or option.cluster_filter(cluster):
    return cluster
  else:
    raise option_value_error

class ClusterOption(Option):
  """A command-line Option that requires a valid cluster name and returns a Cluster object.

  Use in an @app.command_option decorator to avoid boilerplate. For example:

    @app.command
    @app.command_option(ClusterOption('--cluster', default='smf1-test'))
    def get_health(args, options):
      if options.cluster.zk_server:
        do_something(options.cluster)

    @app.command
    @app.command_option(ClusterOption('-s',
      '--source_cluster',
      default='smf1-test',
      help='Source cluster to pull metadata from.'))
    @app.command_option(ClusterOption('-d',
      '--dest_cluster',
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
      default:
        You may specify default as a str or a Cluster. If it's a str it will be converted to Cluster
        and potentially raise a Cluster.UnknownCluster.
        If default is not specified you could get None, so always specify a default if you want to
        assume a value in your command body.
      type: Defaults to a new type, 'mesos_cluster'.
      cluster_filter: A callable that takes a Cluster object and returns truthy if it is valid.
    """
    self.cluster_filter = attrs.pop('cluster_filter', None) # don't pass this along to super
    if callable(self.cluster_filter):
      self._valid_clusters = [c.name for c in Cluster.get_all() if self.cluster_filter(c)]
    else:
      self._valid_clusters = Cluster.get_list()

    default_opt_str = ('--cluster',)
    default_attrs = dict(
      type='mesos_cluster',
      help='Mesos cluster to use (Choose from %s) (Default: %%default)' % self._valid_clusters
    )

    combined_attrs = default_attrs
    combined_attrs.update(attrs) # Defensive copy

    if isinstance(combined_attrs.get('default'), Compatibility.string):
      combined_attrs['default'] = Cluster.get(combined_attrs['default'])

    Option.__init__(self, *opt_str, **combined_attrs) # old-style superclass


  @property
  def valid_clusters(self):
    """List of valid clusters (str keys for cluster.get)."""
    return self._valid_clusters

