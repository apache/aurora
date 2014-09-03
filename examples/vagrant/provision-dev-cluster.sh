#!/bin/bash -x
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

MESOS_VERSION=0.20.0

function prepare_extras() {
  pushd aurora
    # Fetch the mesos egg, needed to build python components.
    mkdir -p third_party
    pushd third_party
      wget -c http://downloads.mesosphere.io/master/ubuntu/14.04/mesos-${MESOS_VERSION}-py2.7-linux-x86_64.egg \
        -O mesos-${MESOS_VERSION}-py2.7-linux-x86_64.egg
    popd

    # Install thrift, needed for code generation in the scheduler build.
    # TODO(wfarner): Move deb file out of jfarrell's individual hosting.
    thrift_deb=thrift-compiler_0.9.1_amd64.deb
    wget -c http://people.apache.org/~jfarrell/thrift/0.9.1/contrib/deb/ubuntu/12.04/$thrift_deb
    dpkg --install $thrift_deb

    # Include build script in default PATH.
    ln -sf /home/vagrant/aurora/examples/vagrant/aurorabuild.sh /usr/local/bin/aurorabuild
  popd

  sudo chown -R vagrant:vagrant aurora

  # Install the upstart configurations.
  cp /vagrant/examples/vagrant/upstart/*.conf /etc/init
}

function install_mesos {
  wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/mesos_${MESOS_VERSION}-1.0.ubuntu1204_amd64.deb
  dpkg --install mesos_${MESOS_VERSION}-1.0.ubuntu1204_amd64.deb
}

function install_cluster_config {
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
  start zookeeper
  start mesos-master
  start mesos-slave
}

function prepare_sources {
  cat > /usr/local/bin/update-sources <<EOF
#!/bin/bash
rsync -urzvh /vagrant/ /home/vagrant/aurora \
    --filter=':- /vagrant/.gitignore' \
    --exclude=.git \
    --delete
EOF
  chmod +x /usr/local/bin/update-sources
  update-sources
}

prepare_sources
install_mesos
prepare_extras
install_cluster_config
start_services
aurorabuild client client2 admin_client executor observer scheduler
