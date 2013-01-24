'''Library of utilities called by the mesos client binary
'''

from __future__ import print_function

import functools
import json
import os
import posixpath
import sys

from pystachio import Ref
from twitter.common import app, log
from twitter.common.contextutil import temporary_dir, open_zip
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import Packer as PackerObject
from twitter.mesos.packer.packer_client import Packer
from twitter.mesos.packer import sd_packer_client
from twitter.mesos.parsers import (
    FormatDetector,
    MesosConfig,
    PystachioCodec,
    PystachioConfig)

from twitter.mesos.client.client_wrapper import MesosClientAPI
from gen.twitter.mesos.constants import LIVE_STATES
from gen.twitter.mesos.ttypes import (
    Identity,
    ResponseCode,
    TaskQuery)


_PACKAGE_FILES_SUFFIX = MesosConfig.PACKAGE_FILES_SUFFIX


def die(msg):
  log.fatal(msg)
  sys.exit(1)


def _zip_package_files(job_name, package_files, tmp_dir):
  zipname = os.path.join(tmp_dir, MesosConfig.get_package_files_zip_name(job_name))
  with open_zip(zipname, 'w') as zipf:
    for file_name in package_files:
      zipf.write(file_name, arcname=os.path.basename(file_name))
  return zipname


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


def _get_and_verify_metadata(package_version):
  if not isinstance(package_version, dict):
    die('packer.get_version did nor return a dict %r' % package_version)
  if not 'id' in package_version:
    die('package_version does not contain an id: %r' % package_version)
  if not 'metadata' in package_version:
    die('package_versions does not contain metadata: %r' % package_version)
  metadata = package_version['metadata']
  if not isinstance(metadata, unicode):
    die('metadata in package_version is not unicode: %r' % package_version)
  try:
    metadata = json.loads(metadata)
  except ValueError:
    die('metadata is package_version is not a valid json object: %r' % package_version)
  if not isinstance(metadata, dict):
    die('deserialized metadata in package_version is not a dict: %r' % package_version)
  if not 'md5sum' in metadata:
    die('metadata in package_version does not have md5sum: %r' % package_version)
  return metadata


def _get_package_uri_from_packer_and_files(cluster, role, name, package_files):
  log.warning('DEVELOPMENT-ONLY FEATURE USED: testing_package_files')
  with temporary_dir(root_dir=os.getcwd()) as tmp_dir:
    packer = sd_packer_client.create_packer(cluster)
    zip_name = _zip_package_files(name, package_files, tmp_dir)
    digest = packer.compute_checksum(zip_name)
    package_name = name + _PACKAGE_FILES_SUFFIX
    package_tuple = (role, package_name, 'latest')
    package_version = None
    metadata = {}
    must_upload = False
    try:
      package_version = packer.get_version(role, package_name, 'latest')
    except Packer.Error:
      must_upload = True  # No package versions present: will upload a new version

    if package_version is not None:
      metadata = _get_and_verify_metadata(package_version)
      must_upload = (metadata['md5sum'] != digest)

    if must_upload:
      log.info('Uploading new version of package_files')
      metadata['md5sum'] = digest
      metadata_json = json.dumps(metadata)
      packer.add(role, package_name, zip_name, metadata_json, digest)
      if package_version is not None:
        # Delete previous version
        packer.delete(role, package_name, package_version['id'])
    else:
      log.info('Not uploading package_files: unchanged package')

  return _extract_package_uri(_get_package_data(cluster, package_tuple, packer))


def _get_package_uri(config):
  cluster = config.cluster()
  # Deprecated PackerPackage object.
  package = config.package()

  # Usage of PackerPackage object has been deprecated.
  # TODO(Sathya Hariesh): Remove this when the deprecation cycle is complete.
  if package:
    return _extract_package_uri(_get_package_data(cluster, package))

  if config.package_files():
    return _get_package_uri_from_packer_and_files(
        cluster, config.role(), config.name(), config.package_files())


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
  if not isinstance(config, PystachioConfig) or not config.package():
    return

  print(PACKAGE_DEPRECATION_WARNING, file=sys.stderr)

  _, refs = config.raw().interpolate()
  p_uri, p = Ref.from_address('mesos.package_uri'), Ref.from_address('mesos.package')
  if p not in refs and p_uri not in refs:
    print(PACKAGE_UNDERSPECIFIED_WARNING % (
        '{{packer[%s][%s][%s].copy_command}}' % tuple(config.package())))


ANNOUNCE_ERROR = """
Announcer specified primary port as '%(primary_port)s' but no processes have bound that port.
If you would like to utilize this port, you must bind {{thermos.ports[%(primary_port)s]}} into
a Process bound in your Task.
"""

def _validate_announce_configuration(config):
  if not config.raw().has_announce():
    return
  primary_port = config.raw().announce().primary_port().get()
  stats_port = config.raw().announce().stats_port().get()
  if primary_port not in config.ports():
    print(ANNOUNCE_ERROR % {'primary_port': primary_port}, file=sys.stderr)
    raise config.InvalidConfig("Announcer specified primary port as "
        '%s but no processes have bound that port!' % primary_port)
  if stats_port not in config.ports():
    raise config.InvalidConfig('Declared stats port "%s" is not bound to a process!' % stats_port)
  if 'aurora' in config.ports() and stats_port != 'aurora':
    raise config.InvalidConfig('Specified named port "aurora" conflicts with stats port "%s"' %
        stats_port)


def _inject_packer_bindings(config, force_local=False):
  if isinstance(config, MesosConfig):
    raise ValueError('inject_packer_bindings can only be used with Pystachio configs!')

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


MESOS_CONFIG_DEPRECATION_MESSAGE = """
You are using a deprecated configuration format.  Please upgrade to the Thermos
configuration format:

  Migration quick guide:
    http://confluence.twitter.biz/display/Aurora/Thermos+Migration+Quick+Guide

  Updated user guide:
    http://confluence.twitter.biz/display/Aurora/User+Guide

  Updated configuration reference:
    http://confluence.twitter.biz/display/Aurora/Aurora+Configuration+Reference
"""

def really_translate(translate=False):
  return os.environ.get('THERMOS_AUTOTRANSLATE', str(translate)).lower() in ('true', '1')


def get_config(jobname,
               config_file,
               json=False,
               force_local=False,
               bindings=(),
               translate=False):
  """Creates and returns a config object contained in the provided file."""
  config_type = 'thermos' if json else FormatDetector.autodetect(config_file)

  if bindings and config_type != 'thermos':
    raise ValueError('Environment bindings only supported for Thermos configs.')

  assert config_type in ('mesos', 'thermos')

  if config_type == 'mesos':
    print(MESOS_CONFIG_DEPRECATION_MESSAGE, file=sys.stderr)
    if really_translate(translate):
      config = PystachioConfig(PystachioCodec(config_file, jobname).build())
    else:
      config = MesosConfig(config_file, jobname)
  else:
    loader = PystachioConfig.load_json if json else PystachioConfig.load
    config = loader(config_file, jobname, bindings)
  return populate_namespaces(config, force_local=force_local)


def populate_namespaces(config, force_local=False):
  """Populate additional bindings in the config, e.g. packer bindings."""
  if isinstance(config, PystachioConfig):
    _inject_packer_bindings(config, force_local)
    _warn_on_unspecified_package_bindings(config)
    _validate_announce_configuration(config)
  return config


def check_and_log_response(resp):
  log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
  if resp.responseCode != ResponseCode.OK:
    sys.exit(1)


class requires(object):
  @staticmethod
  def wrap_function(fn, fnargs, comparator):
    @functools.wraps(fn)
    def wrapped_function(args):
      if not comparator(args, fnargs):
        help = 'Incorrect parameters for %s' % fn.__name__
        if fn.__doc__:
          help = '%s\n\nsee the help subcommand for more details.' % fn.__doc__.split('\n')[0]
        die(help)
      return fn(*args)
    return wrapped_function

  @staticmethod
  def exactly(*args):
    def wrap(fn):
      return requires.wrap_function(fn, args, (lambda want, got: len(want) == len(got)))
    return wrap

  @staticmethod
  def at_least(*args):
    def wrap(fn):
      return requires.wrap_function(fn, args, (lambda want, got: len(want) >= len(got)))
    return wrap

  @staticmethod
  def nothing(fn):
    @functools.wraps(fn)
    def real_fn(line):
      return fn(*line)
    return real_fn


def query_scheduler(api, role, job, shards=None, statuses=LIVE_STATES):
  """Query Aurora Scheduler for Job information."""
  query = TaskQuery()
  query.statuses = statuses
  query.owner = Identity(role=role)
  query.jobName = job
  query.shardIds = shards
  return api.query(query)
