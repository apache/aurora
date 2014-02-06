#
# Copyright 2013 Apache Software Foundation
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

import functools

from apache.aurora.client.hooks.hooked_api import HookedAuroraClientAPI
from apache.aurora.common.cluster import Cluster
from apache.aurora.common.clusters import CLUSTERS

from .base import die

from twitter.common import app


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
