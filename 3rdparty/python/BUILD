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

def make_link(name, version, requirement=None):
  python_requirement_library(
    name = name,
    requirements = [
      python_requirement('%s==%s' % (name, version) if requirement is None else requirement)
    ]
  )

python_requirement_library(
  name = 'cherrypy',
  requirements = [
    python_requirement('cherrypy==3.2.2', repository='http://download.cherrypy.org/cherrypy/3.2.2/')
  ]
)

COMMONS_VERSION = '0.3.0'
MESOS_VERSION = '0.20.1'

make_link('argparse', '1.2.1')
make_link('bottle', '0.11.6')
make_link('Flask', '0.9')
make_link('mako', '0.4.0')
make_link('mesos.interface', MESOS_VERSION)
make_link('mesos.native', MESOS_VERSION)
make_link('mock', '1.0.1')
make_link('mox', '0.5.3')
make_link('psutil', '2.1.3')
make_link('pystachio', '0.7.2')
make_link('pyyaml', '3.10')
make_link('requests', '2.3.0')
make_link('thrift', '0.9.1')
make_link('twitter.common.app', COMMONS_VERSION)
make_link('twitter.common.collections', COMMONS_VERSION)
make_link('twitter.common.concurrent', COMMONS_VERSION)
make_link('twitter.common.config', COMMONS_VERSION)
make_link('twitter.common.confluence', COMMONS_VERSION)
make_link('twitter.common.contextutil', COMMONS_VERSION)
make_link('twitter.common.decorators', COMMONS_VERSION)
make_link('twitter.common.dirutil', COMMONS_VERSION)
make_link('twitter.common.exceptions', COMMONS_VERSION)
make_link('twitter.common.http', COMMONS_VERSION)
make_link('twitter.common.lang', COMMONS_VERSION)
make_link('twitter.common.log', COMMONS_VERSION)
make_link('twitter.common.metrics', COMMONS_VERSION)
make_link('twitter.common.options', COMMONS_VERSION)
make_link('twitter.common.process', COMMONS_VERSION)
make_link('twitter.common.python', '0.5.4')
make_link('twitter.common.quantity', COMMONS_VERSION)
make_link('twitter.common.recordio', COMMONS_VERSION)
make_link('twitter.common.rpc', COMMONS_VERSION)
make_link('twitter.common.string', COMMONS_VERSION)
make_link('twitter.common.testing', COMMONS_VERSION)
make_link('twitter.common.util', COMMONS_VERSION)
make_link('twitter.common.zookeeper', COMMONS_VERSION)
make_link('watchdog', '0.7.0')
