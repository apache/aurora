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

####################################################################################################
# NOTE: When making changes to this file (especially if you are installing new packages), consider
#       instead making changes to the base vagrant box (see build-support/packer/README.md).
####################################################################################################

readonly IP_ADDRESS=192.168.33.7

function prepare_extras() {
  pushd aurora
    # Include build script in default PATH.
    ln -sf /home/vagrant/aurora/examples/vagrant/aurorabuild.sh /usr/local/bin/aurorabuild
  popd
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

function configure_netrc {
  cat > /home/vagrant/.netrc <<EOF
machine $(hostname -f)
login aurora
password secret
EOF
  chown vagrant:vagrant /home/vagrant/.netrc
}

function docker_setup {
  gpasswd -a vagrant docker
  echo 'DOCKER_OPTS="--storage-driver=aufs"' | sudo tee --append /etc/default/docker
  service docker restart
}

function start_services {
  # Executing true on failure to please bash -e in case services are already running
  start zookeeper    || true
}

function prepare_sources {
  # Assign mesos command line arguments.
  cp /vagrant/examples/vagrant/mesos_config/etc_mesos-slave/* /etc/mesos-slave
  cp /vagrant/examples/vagrant/mesos_config/etc_mesos-master/* /etc/mesos-master
  stop mesos-master || true
  stop mesos-slave || true
  # Remove slave metadata to ensure slave start does not pick up old state.
  rm -rf /var/lib/mesos/meta/slaves/latest
  start mesos-master
  start mesos-slave

  cat > /usr/local/bin/update-sources <<EOF
#!/bin/bash
rsync -urzvhl /vagrant/ /home/vagrant/aurora \
    --filter=':- /vagrant/.gitignore' \
    --exclude=.git \
    --exclude=/third_party \
    --delete
# Install/update the upstart configurations.
sudo cp /vagrant/examples/vagrant/upstart/*.conf /etc/init
EOF
  chmod +x /usr/local/bin/update-sources
  update-sources > /dev/null
  chown -R vagrant:vagrant /home/vagrant
}

prepare_sources
prepare_extras
install_cluster_config
install_ssh_config
start_services
configure_netrc
docker_setup
su vagrant -c "aurorabuild all"
