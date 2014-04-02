#!/bin/bash -x
#
# Copyright 2014 Apache Software Foundation
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
    automake \
    curl \
    git \
    g++ \
    libcurl4-openssl-dev \
    libsasl2-dev \
    libtool \
    make \
    openjdk-7-jdk \
    python-dev \
    zookeeper

# Ensure java 7 is the default java.
update-alternatives --set java /usr/lib/jvm/java-7-openjdk-amd64/jre/bin/java

# Set the hostname to the IP address.  This simplifies things for components
# that want to advertise the hostname to the user, or other components.
hostname 192.168.33.7

AURORA_VERSION=$(cat /vagrant/.auroraversion | tr '[a-z]' '[A-Z]')

function build_all() {
  if [ ! -d aurora ]; then
    echo Cloning aurora repo
    git clone /vagrant aurora
  fi

  pushd aurora
    mkdir -p third_party
    pushd third_party
      wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.17.0_amd64.egg \
        -O mesos-0.17.0-py2.7-linux-x86_64.egg
    popd
    git pull

    # build scheduler
    ./gradlew distTar

    # build clients
    ./pants src/main/python/apache/aurora/client/bin:aurora_admin
    ./pants src/main/python/apache/aurora/client/bin:aurora_client

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

function install_mesos() {
  wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_0.17.0_amd64.deb
  dpkg --install mesos_0.17.0_amd64.deb
}

function install_aurora_scheduler() {
  # The bulk of the 'install' was done by the gradle build, the result of which we access
  # through /vagrant.  All that's left is to initialize the log replica.
  tar xvf ~/aurora/dist/distributions/aurora-scheduler-$AURORA_VERSION.tar -C /usr/local

  export LD_LIBRARY_PATH=/usr/lib/jvm/java-7-openjdk-amd64/jre/lib/amd64/server
  if mesos-log initialize --path="/usr/local/aurora-scheduler-$AURORA_VERSION/db"; then
    echo "Replicated log initialized."
  else
    echo "Replicated log initialization failed with code $? (likely already initialized)."
  fi
  unset LD_LIBRARY_PATH
}

function install_aurora_executors() {
  install -m 755 ~/aurora/dist/gc_executor.pex /usr/local/bin/gc_executor
  install -m 755 ~/aurora/dist/thermos_executor.pex /usr/local/bin/thermos_executor
  install -m 755 ~/aurora/dist/thermos_observer.pex /usr/local/bin/thermos_observer
}

function install_aurora_client() {
  install -m 755 /vagrant/dist/aurora_client.pex /usr/local/bin/aurora
  install -m 755 /vagrant/dist/aurora_admin.pex /usr/local/bin/aurora_admin

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

function start_services() {
  cp /vagrant/examples/vagrant/upstart/*.conf /etc/init

  start zookeeper
  start mesos-master
  start mesos-slave
  start aurora-thermos-observer
  start aurora-scheduler
}

build_all
install_mesos
install_aurora_scheduler
install_aurora_executors
install_aurora_client
start_services
