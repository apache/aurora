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

MESOS_REV = '1.4.0'

python_requirement_library(
  name = 'mesos.interface',
  requirements = [
    python_requirement(
      requirement = 'mesos.interface==%s' % MESOS_REV,
    )
  ]
)

python_requirement_library(
  name = 'mesos.executor',
  requirements = [
    python_requirement(
      requirement = 'mesos.executor==%s' % MESOS_REV,
      # This native executor egg must currently be fetched out-of-band.
      # See build-support/packer/build.sh which fetches to third_party/ and uses mesos.executor to
      # enable a running cluster inside the root Vagrant machine.
      repository = '%s/third_party' % get_buildroot(),
    )
  ]
)

python_requirements()
