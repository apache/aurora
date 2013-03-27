'''Library of utilities called by the mesos client binary
'''

from __future__ import print_function

import math
import posixpath
import re
import sys

from twitter.common import app, log
from twitter.mesos.client.base import die
from twitter.mesos.clusters import Cluster
from twitter.mesos.config import AuroraConfig
from twitter.mesos.config.schema import PackerObject
from twitter.mesos.packer.packer_client import Packer
from twitter.mesos.packer import sd_packer_client

from gen.twitter.mesos.constants import LIVE_STATES
from gen.twitter.mesos.ttypes import (
    Identity,
    ResponseCode,
    TaskQuery)

from pystachio import Ref


def _get_package_data(cluster, package, packer=None):
  cluster = Cluster.get(cluster).packer_redirect or cluster
  role, name, version = package
  log.info('Fetching metadata for package %s/%s version %s in %s.' % (
    role, name, version, cluster))
  try:
    if packer is None:
      packer = sd_packer_client.create_packer(cluster)
    return packer.get_version(role, name, version)
  except Packer.Error as e:
    die('Failed to fetch package metadata: %s' % e)


def _extract_package_uri(metadata):
  latest_audit = sorted(metadata['auditLog'], key=lambda a: a['timestamp'])[-1]
  if latest_audit['state'] == 'DELETED':
    die('The requested package version has been deleted.')
  return metadata['uri']


APPAPP_DEPRECATION_WARNING = """
The use of app-app is deprecated. Please reach out to mesos-team@twitter.com for advice on
migrating your application away from app-app layouts to an alternative packaging solution.
"""
def _warn_on_appapp_layouts(config):
  if config.raw().has_layout():
    print(APPAPP_DEPRECATION_WARNING, file=sys.stderr)


PACKAGE_DEPRECATION_WARNING = """
Job.package is deprecated.  Instead use the {{packer}} namespace directly.

See the packer section of the Configuration Reference page for more information:
http://confluence.twitter.biz/display/Aurora/Aurora+Configuration+Reference#AuroraConfigurationReference-%7B%7Bpacker%7D%7Dnamespace
"""


PACKAGE_UNDERSPECIFIED_WARNING = """
You've specified Job.package in your configuration but not referenced {{mesos.package}}
or {{mesos.package_uri}}.  We no longer copy package artifacts directly into your
sandbox prior to invocation, so you must copy them by adding:

  %s

either as a process or into a process in your Task.
"""


def _warn_on_unspecified_package_bindings(config):
  if not config.package():
    return

  print(PACKAGE_DEPRECATION_WARNING, file=sys.stderr)

  _, refs = config.raw().interpolate()
  p_uri, p = Ref.from_address('mesos.package_uri'), Ref.from_address('mesos.package')
  if p not in refs and p_uri not in refs:
    print(PACKAGE_UNDERSPECIFIED_WARNING % (
        '{{packer[%s][%s][%s].copy_command}}' % tuple(config.package())), file=sys.stderr)


CRON_DEPRECATION_WARNING = """
The "cron_policy" parameter to Jobs has been renamed to "cron_collision_policy".
Please update your Jobs accordingly.
"""

def _warn_on_deprecated_cron_policy(config):
  if config.raw().cron_policy() is not Empty:
    print(CRON_DEPRECATION_WARNING, file=sys.stderr)


DAEMON_DEPRECATION_WARNING = """
The "daemon" parameter to Jobs is deprecated in favor of the "service" parameter.
Please update your Job to set "service = True" instead of "daemon = True", or use
the top-level Service() instead of Job().
"""

def _warn_on_deprecated_daemon_job(config):
  if config.raw().daemon() is not Empty:
    print(DAEMON_DEPRECATION_WARNING, file=sys.stderr)


ANNOUNCE_WARNING = """
Announcer specified primary port as '%(primary_port)s' but no processes have bound that port.
If you would like to utilize this port, you should listen on {{thermos.ports[%(primary_port)s]}}
from some Process bound to your task.
"""

def _validate_announce_configuration(config):
  if not config.raw().has_announce():
    return

  primary_port = config.raw().announce().primary_port().get()
  if primary_port not in config.ports():
    print(ANNOUNCE_WARNING % {'primary_port': primary_port}, file=sys.stderr)

  if config.raw().has_announce() and not config.raw().has_constraints() or (
      'dedicated' not in config.raw().constraints()):
    for port in config.raw().announce().portmap().get().values():
      try:
        port = int(port)
      except ValueError:
        continue
      raise ValueError('Job must be dedicated in order to specify static ports!')


STAGING_RE = re.compile(r'^staging\d*$')

def _validate_environment_name(config):
  if not config.raw().has_environment():
    return
  env_name = str(config.raw().environment())
  if STAGING_RE.match(env_name):
    return
  if env_name not in ('prod', 'devel', 'test'):
    raise ValueError('Environment name should be one of "prod", "devel", "test" or '
                     'staging<number>!  Got %s' % env_name)


UPDATE_CONFIG_MAX_FAILURES_ERROR = '''
max_total_failures in update_config must be lesser than the job size.
Based on your job size (%s) you should use max_total_failures <= %s.

See http://go/auroraconfig for details.
'''

UPDATE_CONFIG_DEDICATED_THRESHOLD_ERROR = '''
Since this is a dedicated job, you must set your max_total_failures in
your update configuration to no less than 2%% of your job size.
Based on your job size (%s) you should use max_total_failures >= %s.

See http://go/auroraconfig for details.
'''

def _validate_update_config(config):
  job_size = config.instances()
  max_failures = config.update_config().max_total_failures().get()

  if max_failures >= job_size:
    die(UPDATE_CONFIG_MAX_FAILURES_ERROR % (job_size, job_size - 1))

  if config.is_dedicated():
    min_failure_threshold = int(math.floor(job_size * 0.02))
    if max_failures < min_failure_threshold:
      die(UPDATE_CONFIG_DEDICATED_THRESHOLD_ERROR % (job_size, min_failure_threshold))


def _inject_packer_bindings(config, force_local=False):
  local = config.cluster() == 'local' or force_local

  def extract_ref(ref):
    components = ref.components()
    if len(components) < 4:
      return None
    if components[0] != Ref.Dereference('packer'):
      return None
    if not all(isinstance(action, Ref.Index) for action in components[1:4]):
      return None
    role, package_name, version = (action.value for action in components[1:4])
    return (role, package_name, version)

  def generate_packer_struct(uri):
    packer = PackerObject(
      tunnel_host=app.get_options().tunnel_host,
      package=posixpath.basename(uri),
      package_uri=uri)
    packer = packer(copy_command=packer.local_copy_command() if local
                    else packer.remote_copy_command())
    return packer

  _, refs = config.raw().interpolate()
  packages = filter(None, map(extract_ref, set(refs)))
  for package in set(packages):
    ref = Ref.from_address('packer[%s][%s][%s]' % package)
    package_data = _get_package_data(config.cluster(), package)
    config.bind({ref: generate_packer_struct(_extract_package_uri(package_data))})
    config.add_package((package[0], package[1], package_data['id']))


def get_config(jobname,
               config_file,
               json=False,
               force_local=False,
               bindings=(),
               select_cluster=None,
               select_env=None):
  """Creates and returns a config object contained in the provided file."""
  loader = AuroraConfig.load_json if json else AuroraConfig.load
  config = loader(config_file,
                  jobname,
                  bindings,
                  select_cluster=select_cluster,
                  select_env=select_env)
  validate_config(config)
  return populate_namespaces(config, force_local=force_local)


def validate_config(config):
  _validate_update_config(config)
  _validate_announce_configuration(config)
  _validate_environment_name(config)


def populate_namespaces(config, force_local=False):
  """Populate additional bindings in the config, e.g. packer bindings."""
  _inject_packer_bindings(config, force_local)
  _warn_on_unspecified_package_bindings(config)
  _warn_on_deprecated_cron_policy(config)
  _warn_on_deprecated_daemon_job(config)
  _warn_on_appapp_layouts(config)
  return config
