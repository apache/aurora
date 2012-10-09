'''Library of utilities called by the mesos client binary
'''

import functools
import json
import os
import posixpath
import shutil
import sys
import tempfile
import zipfile

from pystachio import Ref
from urlparse import urljoin
from twitter.common import app, log
from twitter.common.contextutil import temporary_dir, open_zip
from twitter.mesos.clusters import Cluster
from twitter.mesos.config.schema import Packer as PackerObject
from twitter.mesos.packer.packer_client import Packer
from twitter.mesos.packer import sd_packer_client
from twitter.mesos.parsers.mesos_config import MesosConfig
from twitter.mesos.parsers.pystachio_config import PystachioConfig
from twitter.mesos.parsers.pystachio_codec import PystachioCodec
from twitter.mesos.client.spawn_local import (
  LocalDriver,
  build_local_runner,
  create_executor,
  create_taskinfo,
  spawn_observer)

from gen.twitter.mesos.ttypes import *


_PACKAGE_FILES_SUFFIX = '__TESTING_package_files'


def die(msg):
  log.fatal(msg)
  sys.exit(1)


def open_url(url):
  if url is not None:
    import webbrowser
    webbrowser.open_new_tab(url)


def synthesize_url(scheduler_client, role=None, job=None):
  scheduler_url = scheduler_client.url
  if not scheduler_url:
    log.warning("Unable to find scheduler web UI!")
    return None

  if job and not role:
    die('If job specified, must specify role!')

  if not role and not job:
    return urljoin(scheduler_url, 'scheduler')
  elif role and not job:
    return urljoin(scheduler_url, 'scheduler/%s' % role)
  else:
    return urljoin(scheduler_url, 'scheduler/%s/%s' % (role, job))


def handle_open(scheduler_client, role, job):
  url = synthesize_url(scheduler_client, role, job)
  if url:
    log.info('Job url: %s' % url)
    if app.get_options().open_browser:
      open_url(url)


def _zip_package_files(job_name, package_files, tmp_dir):
  zipname = os.path.join(tmp_dir, job_name + _PACKAGE_FILES_SUFFIX + '.zip')
  with open_zip(zipname, 'w') as zipf:
    for file_name in package_files:
      zipf.write(file_name, arcname=os.path.basename(file_name))
  return zipname


def _get_package_uri_from_packer(cluster, package, packer=None):
  cluster = Cluster.get(cluster).packer_redirect or cluster
  role, name, version = package
  log.info('Fetching metadata for package %s/%s version %s in %s.' % (
    role, name, version, cluster))
  try:
    if packer is None:
      packer = sd_packer_client.create_packer(cluster)
    metadata = packer.get_version(role, name, version)
  except Packer.Error as e:
    die('Failed to fetch package metadata: %s' % e)

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
  log.warning('DEVELOPMENT-ONLY FEATURE USED: TESTING_package_files')
  with temporary_dir(root_dir=os.getcwd()) as tmp_dir:
    packer = sd_packer_client.create_packer(cluster)
    zip_name = _zip_package_files(name, package_files, tmp_dir)
    digest = Packer.compute_checksum(zip_name)
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

  return _get_package_uri_from_packer(cluster, package_tuple, packer)


def _get_package_uri(config):
  cluster = config.cluster()
  package = config.package()
  package_files = config.package_files()

  if config.hdfs_path():
    log.warning('''
*******************************************************************************
  hdfs_path in job configurations has been deprecated and will soon be
  disabled altogether.
  Please switch to using the package option as soon as possible!
  For details on how to do this, please consult
  http://go/mesostutorial
  and
  http://confluence.local.twitter.com/display/ENG/Mesos+Configuration+Reference
*******************************************************************************''')

  options = app.get_options()
  if package and options.copy_app_from:
    die('copy_app_from may not be used when a package spec is used in the configuration')

  if package:
    return _get_package_uri_from_packer(cluster, package)

  if package_files:
    return _get_package_uri_from_packer_and_files(
        cluster, config.role(), config.name(), package_files)

  if config.hdfs_path():
    return config.hdfs_path()

  if options.copy_app_from:
    return '/mesos/pkg/%s/%s' % (config.role(), posixpath.basename(options.copy_app_from))


def _inject_packer_bindings(config, local=False):
  if not isinstance(config, PystachioConfig):
    raise ValueError('inject_packer_bindings can only be used with Pystachio configs!')

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
    config.bind({ref: generate_packer_struct(
      _get_package_uri_from_packer(config.cluster(), package))})


def get_config(jobname, config_file, local=False):
  """Creates and returns a config object contained in the provided file."""
  options = app.get_options()
  config_type, is_json = options.config_type, options.json
  bindings = getattr(options, 'bindings', [])

  if is_json:
    assert config_type == 'thermos', "--json only supported with thermos jobs"

  if config_type == 'mesos':
    config = MesosConfig(config_file, jobname)
  elif config_type == 'thermos':
    loader = PystachioConfig.load_json if is_json else PystachioConfig.load
    config = loader(config_file, jobname, bindings)
    _inject_packer_bindings(config, local=local)
  elif config_type == 'auto':
    config = PystachioCodec(config_file, jobname)
  else:
    raise ValueError('Unknown config type %s!' % config_type)

  package_uri = _get_package_uri(config)
  if package_uri:
    config.set_hdfs_path(package_uri)
  return config


def check_and_log_response(resp):
  log.info('Response from scheduler: %s (message: %s)'
      % (ResponseCode._VALUES_TO_NAMES[resp.responseCode], resp.message))
  if resp.responseCode != ResponseCode.OK:
    sys.exit(1)


def really_spawn(jobname, config_file, options):
  config = get_config(jobname, config_file, local=True)
  if not isinstance(config, PystachioConfig):
    app.error('spawn command only works with new-style Thermos tasks.')

  checkpoint_root = os.path.expanduser(os.path.join('~', '.thermos'))
  server_thread, port = spawn_observer(checkpoint_root)
  task_info = create_taskinfo(config, options.shard)
  sandbox = tempfile.mkdtemp()

  runner_pex = options.runner if options.runner != 'build' else build_local_runner()
  if runner_pex is None:
    app.error('failed to build thermos runner!')

  executor = create_executor(runner_pex, sandbox, checkpoint_root)
  driver = LocalDriver()
  executor.launchTask(driver, task_info)

  if options.open_browser:
    open_url('http://localhost:%d/task/%s' % (port, task_info.task_id.value))

  try:
    driver.stopped.wait()
  except KeyboardInterrupt:
    print('Got interrupt, killing task.')

  executor.shutdown(driver)
  print('Local spawn completed.')
  return 0


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
