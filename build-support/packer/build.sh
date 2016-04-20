#!/bin/bash
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

set -o errexit
set -o nounset
set -o verbose

readonly MESOS_VERSION=0.27.2

function remove_unused {
  # The default ubuntu/trusty64 image includes juju-core, which adds ~300 MB to our image.
  apt-get purge -y --auto-remove juju-core

  rm -f /home/vagrant/VBoxGuestAdditions.iso
}

function install_base_packages {
  add-apt-repository ppa:openjdk-r/ppa -y
  apt-get update
  apt-get -y install \
      bison \
      curl \
      git \
      jq \
      libapr1-dev \
      libcurl4-nss-dev \
      libsasl2-dev \
      libsvn-dev \
      openjdk-8-jdk \
      python-dev
  update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java
  # Installing zookeeperd as a separate command, as otherwise openjdk-7-jdk is also installed.
  apt-get install -y zookeeperd
}

function install_docker {
  # Instructions from https://docs.docker.com/engine/installation/linux/ubuntulinux/
  apt-get install -y apt-transport-https ca-certificates
  apt-key adv --keyserver hkp://p80.pool.sks-keyservers.net:80 \
    --recv-keys 58118E89F3A912897C070ADBF76221572C52609D
  echo 'deb https://apt.dockerproject.org/repo ubuntu-trusty main' \
    > /etc/apt/sources.list.d/docker.list
  apt-get update
  apt-get -y install \
    linux-image-extra-$(uname -r) \
    apparmor \
    docker-engine
}

function install_mesos {
  URL_BASE='http://repos.mesosphere.com/ubuntu/pool/main/m/mesos'
  DEB_URL="$URL_BASE/mesos_${MESOS_VERSION}-2.0.15.ubuntu1404_amd64.deb"
  deb=$(basename $DEB_URL)
  wget -c "$DEB_URL"
  dpkg --install $deb
  rm $deb
}

function install_thrift {
  # Install thrift, needed for code generation in the scheduler build.
  curl -sSL http://apache.org/dist/thrift/KEYS | gpg --import -
  gpg --export --armor 66B778F9 | apt-key add -
  echo 'deb http://www.apache.org/dist/thrift/debian 0.9.1 main' \
    > /etc/apt/sources.list.d/thrift.list
  apt-get update
  apt-get install thrift-compiler=0.9.1
}

function warm_artifact_cache {
  # Gradle caches in the user's home directory.  Since development commands
  # are executed by the vagrant user, switch to that user.
  su - vagrant -c '
    git clone --depth 1 https://github.com/apache/aurora.git
    pushd aurora
      ./build-support/jenkins/build.sh
    popd
    rm -rf aurora'

  THIRD_PARTY_DIR=/home/vagrant/aurora/third_party
  mkdir -p "$THIRD_PARTY_DIR"

  # Fetch the mesos egg, needed to build python components.
  # The mesos.native target in 3rdparty/python/BUILD expects to find the native egg in third_party.
  SVN_ROOT='https://svn.apache.org/repos/asf/aurora/3rdparty'
  pushd "$THIRD_PARTY_DIR"
    wget -c ${SVN_ROOT}/ubuntu/trusty64/python/mesos.native-${MESOS_VERSION}-py2.7-linux-x86_64.egg
  popd

  chown -R vagrant:vagrant aurora
}

function compact_box {
  apt-get clean

  # By design, this will fail as it writes until the disk is full.
  dd if=/dev/zero of=/junk bs=1M || true
  rm -f /junk
  sync
}

remove_unused
install_base_packages
install_docker
install_mesos
install_thrift
warm_artifact_cache
compact_box
