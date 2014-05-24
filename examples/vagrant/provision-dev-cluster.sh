#!/bin/bash -x
#
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

apt-get update
apt-get -y install \
    curl \
    git \
    libcurl4-openssl-dev \
    libsasl2-dev \
    openjdk-7-jdk \
    python-dev \
    zookeeper

# Ensure java 7 is the default java.
update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java

# Set the hostname to the IP address.  This simplifies things for components
# that want to advertise the hostname to the user, or other components.
hostname 192.168.33.7

function build_all() {
  echo Copying aurora source code
  rsync -urzvh /vagrant/ aurora \
      --exclude '.gradle' \
      --exclude '.pants.d' \
      --exclude 'dist/*' \
      --exclude 'build-support/*.venv' \
      --exclude 'build-support/thrift/thrift-*' \
      --exclude '*.pyc'

  pushd aurora
    # fetch the mesos egg, needed to build python components
    mkdir -p third_party
    pushd third_party
      wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.18.0_amd64.egg \
        -O mesos-0.18.0-py2.7-linux-x86_64.egg
    popd

    # install thrift, needed for code generation in the scheduler build
    # TODO(wfarner): Move deb file out of jfarrell's individual hosting.
    thrift_deb=thrift-compiler_0.9.1_amd64.deb
    wget -c http://people.apache.org/~jfarrell/thrift/0.9.1/contrib/deb/ubuntu/12.04/$thrift_deb
    dpkg --install $thrift_deb

    # build scheduler
    ./gradlew installApp

    # build clients
    ./pants src/main/python/apache/aurora/client/bin:aurora_admin
    ./pants src/main/python/apache/aurora/client/bin:aurora_client
    ./pants src/main/python/apache/aurora/client/cli:aurora2

    # build executors/observers
    ./pants src/main/python/apache/aurora/executor/bin:gc_executor
    ./pants src/main/python/apache/aurora/executor/bin:thermos_executor
    ./pants src/main/python/apache/aurora/executor/bin:thermos_runner
    ./pants src/main/python/apache/thermos/observer/bin:thermos_observer

    # package runner w/in executor
    python <<EOF
import contextlib
import zipfile
with contextlib.closing(zipfile.ZipFile('dist/thermos_executor.pex', 'a')) as zf:
  zf.writestr('apache/aurora/executor/resources/__init__.py', '')
  zf.write('dist/thermos_runner.pex', 'apache/aurora/executor/resources/thermos_runner.pex')
EOF
  popd

  sudo chown -R vagrant:vagrant aurora
}

function install_mesos {
  wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.18.0_amd64.deb
  dpkg --install mesos_0.18.0_amd64.deb
}

function install_aurora {
  # The bulk of the 'install' was done by the build, the result of which we access
  # through /home/vagrant.
  DIST_DIR=/home/vagrant/aurora/dist
  AURORA_HOME=/usr/local/aurora

  export LD_LIBRARY_PATH=/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server
  mkdir -p $AURORA_HOME/scheduler
  if mesos-log initialize --path="$AURORA_HOME/scheduler/db"; then
    echo "Replicated log initialized."
  else
    echo "Replicated log initialization failed with code $? (likely already initialized)."
  fi
  unset LD_LIBRARY_PATH

  # Ensure clients are in the default PATH.
  ln -s $DIST_DIR/aurora_client.pex /usr/local/bin/aurora
  ln -s $DIST_DIR/aurora2.pex /usr/local/bin/aurora2
  ln -s $DIST_DIR/aurora_admin.pex /usr/local/bin/aurora_admin

  mkdir -p /etc/aurora
  cat > /etc/aurora/clusters.json <<EOF
[{
  "name": "devcluster",
  "zk": "192.168.33.7",
  "scheduler_zk_path": "/aurora/scheduler",
  "auth_mechanism": "UNAUTHENTICATED",
  "slave_run_directory": "latest",
  "slave_root": "/var/lib/mesos"
}]
EOF
}

function start_services {
  cp /vagrant/examples/vagrant/upstart/*.conf /etc/init

  start zookeeper
  start mesos-master
  start mesos-slave
  start aurora-thermos-observer
  start aurora-scheduler
}

build_all
install_mesos
install_aurora
start_services
