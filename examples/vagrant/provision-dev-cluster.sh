#!/bin/bash -ex
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

apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 36A1D7869245C8950F966E92D8576A8BA88D21E9
echo deb https://get.docker.com/ubuntu docker main > /etc/apt/sources.list.d/docker.list
add-apt-repository ppa:openjdk-r/ppa -y
apt-get update
apt-get -y install \
    bison \
    curl \
    git \
    libapr1-dev \
    libcurl4-nss-dev \
    libsasl2-dev \
    libsvn-dev \
    lxc-docker \
    openjdk-8-jdk \
    python-dev \
    zookeeperd

update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

readonly IP_ADDRESS=192.168.33.7

readonly MESOS_VERSION=0.24.1

function prepare_extras() {
  pushd aurora
    # Fetch the mesos egg, needed to build python components.
    # The mesos.native target in 3rdparty/python/BUILD expects to find the native egg in third_party.
    mkdir -p third_party
    pushd third_party
      wget -c https://svn.apache.org/repos/asf/aurora/3rdparty/ubuntu/trusty64/python/mesos.native-${MESOS_VERSION}-py2.7-linux-x86_64.egg
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
}

function install_mesos {
  deb=mesos_${MESOS_VERSION}-0.2.35.ubuntu1204_amd64.deb
  wget -c http://downloads.mesosphere.io/master/ubuntu/12.04/$deb
  dpkg --install $deb
}

function install_cluster_config {
  mkdir -p /etc/aurora
  ln -sf /home/vagrant/aurora/examples/vagrant/clusters.json /etc/aurora/clusters.json
}

function install_ssh_config {
  cat >> /etc/ssh/ssh_config <<EOF

# Allow local ssh w/out strict host checking
Host *
    StrictHostKeyChecking no
    UserKnownHostsFile /dev/null
EOF
}

function enable_gradle_daemon {
  install -o vagrant -g vagrant -d -m 0755 /home/vagrant/.gradle
  cat > /home/vagrant/.gradle/gradle.properties <<EOF
org.gradle.daemon=true
EOF
  chown vagrant:vagrant /home/vagrant/.gradle/gradle.properties
}

function configure_netrc {
  cat > /home/vagrant/.netrc <<EOF
machine $(hostname -f)
login aurora
password secret
EOF
  chown vagrant:vagrant /home/vagrant/.netrc
}

function sudoless_docker_setup {
  gpasswd -a vagrant docker
  service docker restart
}

function start_services {
  #Executing true on failure to please bash -e in case services are already running
  start zookeeper    || true
  start mesos-master || true
  start mesos-slave  || true
}

function prepare_sources {
  cat > /usr/local/bin/update-sources <<EOF
#!/bin/bash
rsync -urzvhl /vagrant/ /home/vagrant/aurora \
    --filter=':- /vagrant/.gitignore' \
    --exclude=.git \
    --delete
# Install/update the upstart configurations.
sudo cp /vagrant/examples/vagrant/upstart/*.conf /etc/init
EOF
  chmod +x /usr/local/bin/update-sources
  update-sources
}

install_mesos
prepare_sources
prepare_extras
install_cluster_config
install_ssh_config
start_services
enable_gradle_daemon
configure_netrc
sudoless_docker_setup
su vagrant -c "aurorabuild all"
