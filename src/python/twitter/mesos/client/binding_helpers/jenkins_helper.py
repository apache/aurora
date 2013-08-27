from twitter.mesos.client.jenkins import JenkinsArtifactResolver
from twitter.packer import sd_packer_client

from .packer_helper import generate_packer_struct, validate_package
from . import BindingHelper

from pystachio import Ref
from pystachio.matcher import Any, Matcher


class JenkinsBindingHelper(BindingHelper):
  @classmethod
  def get_ref_kind(self):
    return 'jenkins'

  def get_ref_pattern(self):
    return Matcher('jenkins')[Any][Any]

  def bind_ref(self, config, match, env, force_local, binding_dict):
    def get_package_via_jenkins(cluster, role, package):
      packer = sd_packer_client.create_packer(cluster)
      return JenkinsArtifactResolver(packer, role).resolve(*package)

    if binding_dict is None:
      binding_dict = {}
    local = config.cluster() == 'local' or force_local
    jenkins_project, jenkins_build_number = match[1:]
    ref_str = 'jenkins[%s][%s]' % (jenkins_project, jenkins_build_number)
    if ref_str in binding_dict:
      package_name = binding_dict[ref_str]['name']
      package_data = binding_dict[ref_str]['data']
    else:
      package_name, package_data = get_package_via_jenkins(
          config.cluster(), config.role(), match[1:3])
      binding_dict[ref_str] = {'name': package_name, 'data': package_data}
    config.bind({Ref.from_address(ref_str):
          generate_packer_struct(validate_package(package_data), local)})
    config.add_package((config.role(), package_name, package_data['id']))
