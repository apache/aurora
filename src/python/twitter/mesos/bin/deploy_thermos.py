import os
import subprocess
import sys
import time

from twitter.common import app
from twitter.mesos.clusters import Cluster
from twitter.mesos.deploy import (
  Builder,
  HDFSDeployer)


class ThermosBuilder(Builder):
  @property
  def project(self):
    return 'thermos'

  @property
  def commands(self):
    return [
      './pants src/python/twitter/mesos/executor:thermos_executor',
      './pants src/python/twitter/mesos/executor:thermos_runner',
      './pants src/python/twitter/mesos/executor:gc_executor',
    ]

  @property
  def artifacts(self):
    return {
      'dist/thermos_executor.pex': '$cluster/thermos_executor.pex',
      'dist/gc_executor.pex': '$cluster/gc_executor.pex',
    }

  def preprocess(self):
    self.check_call('rm -f pants.pex')

  def postprocess(self):
    import contextlib
    import zipfile
    with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
      zf.writestr('twitter/mesos/executor/resources/__init__.py', '')
      zf.write('dist/thermos_runner.pex', 'twitter/mesos/executor/resources/thermos_runner.pex')


app.set_usage('%prog [options] tag')

cluster_list = Cluster.get_list()
app.add_option('--cluster', type = 'choice', choices = cluster_list, dest='cluster',
               help='Cluster to deploy the scheduler in (one of: %s)' % ', '.join(cluster_list))

app.add_option('-v', dest='verbose', default=False, action='store_true',
               help='Verbose logging.')

app.add_option('--really_push', dest='really_push', default=False, action='store_true',
               help='Safeguard to prevent fat-fingering.  When false, only show commands but do '
                    'not run them.')

app.add_option('--hotfix', dest='hotfix', default=False, action='store_true',
               help='Indicates this is a hotfix deploy from the current tree.')

app.add_option('--release', dest='release', default=None, type=int,
               help='Specify a release number to deploy. If none specified, it uses the latest '
                    'release assigned to the cluster environment.  If specified, it must have '
                    'been assigned as a release for this environment.')


def main(_, options):
  if not options.cluster:
    cluster_list = Cluster.get_list()
    print ('Please specify the cluster you would like to deploy to with\n\t--cluster %s'
           % cluster_list)
    return

  builder = ThermosBuilder(options.cluster, options.release, options.hotfix, options.verbose)
  builder.build()

  deployer = HDFSDeployer(options.cluster, not options.really_push, options.verbose)
  deployer.stage(builder)


app.main()
