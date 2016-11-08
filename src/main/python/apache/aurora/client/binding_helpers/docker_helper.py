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

from pystachio import Ref
from pystachio.matcher import Any, Matcher

from apache.aurora.client.binding_helper import BindingHelper
from apache.aurora.client.docker.docker_client import DockerRegistryClient
from apache.aurora.common.clusters import CLUSTERS


class DockerBindingHelper(BindingHelper):
  @property
  def name(self):
    return 'docker'

  @property
  def matcher(self):
    return Matcher('docker').image[Any][Any]

  def bind(self, config, match, env, binding_dict):
    cluster = CLUSTERS[config.cluster()]
    image = match
    ref_str = 'docker.image[%s][%s]' % image[2:4]
    ref = Ref.from_address(ref_str)
    if ref_str in binding_dict:
      (image_data, image_struct) = binding_dict[ref_str]
    else:
      image_data = '%s:%s' % (image[2], image[3])
      image_struct = DockerRegistryClient.resolve(cluster, image[2], image[3])
    binding_dict[ref_str] = (image_data, image_struct)
    config.bind({ref: image_struct})
