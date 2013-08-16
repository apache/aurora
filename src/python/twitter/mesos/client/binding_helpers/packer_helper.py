import posixpath

from twitter.common import app, log
from twitter.mesos.client.base import die
from twitter.mesos.config.schema import PackerObject
from twitter.mesos.packer import sd_packer_client
from twitter.mesos.packer.packer_client import Packer

from . import BindingHelper

from pystachio import Ref
from pystachio.matcher import Any, Matcher


def generate_packer_struct(metadata, local):
  uri = metadata['uri']
  filename = metadata.get('filename', None) or posixpath.basename(uri)
  packer = PackerObject(
      tunnel_host=app.get_options().tunnel_host,
      package=filename,
      package_uri=uri)
  packer = packer(copy_command=packer.local_copy_command() if local
  else packer.remote_copy_command())
  return packer


def validate_package(metadata):
  latest_audit = sorted(metadata['auditLog'], key=lambda a: a['timestamp'])[-1]
  if latest_audit['state'] == 'DELETED':
    die('The requested package version has been deleted.')
  return metadata


def get_package_data(cluster, package):
  role, name, version = package
  log.info('Fetching metadata for package %s/%s version %s in %s.' % (
      role, name, version, cluster))
  try:
    # TODO(wickman) MESOS-3006
    packer = sd_packer_client.create_packer(cluster)
    return packer.get_version(role, name, version)
  except Packer.Error as e:
    die('Failed to fetch package metadata: %s' % e)


class PackerBindingHelper(BindingHelper):
  @classmethod
  def get_ref_kind(self):
    return 'packer'

  def get_ref_pattern(self):
    return Matcher('packer')[Any][Any][Any]

  def bind_ref(self, config, match, env, force_local, binding_dict):
    local = config.cluster() == 'local' or force_local
    package = match
    ref_str = 'packer[%s][%s][%s]' % package[1:4]
    ref = Ref.from_address(ref_str)
    if ref_str in binding_dict:
      (package_data, packer_struct) = binding_dict[ref_str]
    else:
      package_data = get_package_data(config.cluster(), package[1:4])
      packer_struct = generate_packer_struct(validate_package(package_data), local)
    binding_dict[ref_str] = (package_data, packer_struct)
    config.bind({ref: packer_struct})
    config.add_package((package[1], package[2], package_data['id']))
