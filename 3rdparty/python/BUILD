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

COMMONS_VERSION = '0.3.0'

python_requirement('argparse==1.2.1')
python_requirement('bottle==0.11.6')
# As of twitter.pants==0.0.23, we no longer pull in dependencies transitively from
# pypi (the equivalent of --allow-external in pip.)  We must specify explicitly
# the repository link for anything whose distribution is not available directly from
# repos specified in pants.ini.
python_requirement(
    'cherrypy==3.2.2',
    repository='http://download.cherrypy.org/cherrypy/3.2.2/')
python_requirement('Flask==0.9')
python_requirement('mako==0.4.0')
python_requirement('mesos==0.18.0')
python_requirement('mock==1.0.1')
python_requirement('mox==0.5.3')
python_requirement('psutil==1.1.2')
python_requirement('pystachio==0.7.2')
python_requirement('pyyaml==3.10')
python_requirement('requests==2.0.0')
python_requirement('thrift==0.9.1')
python_requirement('twitter.common.app==%s' % COMMONS_VERSION)
python_requirement('twitter.common.collections==%s' % COMMONS_VERSION)
python_requirement('twitter.common.concurrent==%s' % COMMONS_VERSION)
python_requirement('twitter.common.config==%s' % COMMONS_VERSION)
python_requirement('twitter.common.confluence==%s' % COMMONS_VERSION)
python_requirement('twitter.common.contextutil==%s' % COMMONS_VERSION)
python_requirement('twitter.common.decorators==%s' % COMMONS_VERSION)
python_requirement('twitter.common.dirutil==%s' % COMMONS_VERSION)
python_requirement('twitter.common.exceptions==%s' % COMMONS_VERSION)
python_requirement('twitter.common.http==%s' % COMMONS_VERSION)
python_requirement('twitter.common.lang==%s' % COMMONS_VERSION)
python_requirement('twitter.common.log==%s' % COMMONS_VERSION)
python_requirement('twitter.common.metrics==%s' % COMMONS_VERSION)
python_requirement('twitter.common.options==%s' % COMMONS_VERSION)
python_requirement('twitter.common.process==%s' % COMMONS_VERSION)
python_requirement('twitter.common.python==0.5.4')
python_requirement('twitter.common.quantity==%s' % COMMONS_VERSION)
python_requirement('twitter.common.recordio==%s' % COMMONS_VERSION)
python_requirement('twitter.common.rpc==%s' % COMMONS_VERSION)
python_requirement('twitter.common.string==%s' % COMMONS_VERSION)
python_requirement('twitter.common.testing==%s' % COMMONS_VERSION)
python_requirement('twitter.common.util==%s' % COMMONS_VERSION)
python_requirement('twitter.common.zookeeper==%s' % COMMONS_VERSION)
python_requirement('watchdog==0.7.0')
