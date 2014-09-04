# -*- mode: ruby -*-
# vi: set ft=ruby :
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

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# 1.5.0 is required to use vagrant cloud images.
# https://www.vagrantup.com/blog/vagrant-1-5-and-vagrant-cloud.html
Vagrant.require_version ">= 1.5.0"

Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.define "devcluster" do |dev|
    dev.vm.network :private_network, ip: "192.168.33.7"
    dev.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1024"]
    end
    dev.vm.provision "shell", path: "examples/vagrant/provision-dev-cluster.sh"
  end
end
