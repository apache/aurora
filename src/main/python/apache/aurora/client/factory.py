import functools

from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import CLUSTERS
from twitter.common import app

from .base import die


# TODO(wickman) Kill make_client and make_client_factory as part of MESOS-3801.
# These are currently necessary indirections for the LiveJobDisambiguator among
# other things but can go away once those are scrubbed.

def make_client_factory():
  verbose = getattr(app.get_options(), 'verbosity', 'normal') == 'verbose'
  class TwitterAuroraClientAPI(HookedAuroraClientAPI):
    def __init__(self, cluster, *args, **kw):
      if cluster not in CLUSTERS:
        die('Unknown cluster: %s' % cluster)
      super(TwitterAuroraClientAPI, self).__init__(CLUSTERS[cluster], *args, **kw)
  return functools.partial(TwitterAuroraClientAPI, verbose=verbose)


def make_client(cluster):
  factory = make_client_factory()
  return factory(cluster.name if isinstance(cluster, Cluster) else cluster)
