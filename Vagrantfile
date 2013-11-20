# -*- mode: ruby -*-
# vi: set ft=ruby :

# Vagrantfile API/syntax version. Don't touch unless you know what you're doing!
VAGRANTFILE_API_VERSION = "2"

# TODO(ksweeney): RAM requirements are not empirical and can probably be significantly lowered.
Vagrant.configure(VAGRANTFILE_API_VERSION) do |config|
  config.vm.box = "precise64"

  # The url from where the 'config.vm.box' box will be fetched if it
  # doesn't already exist on the user's system.
  config.vm.box_url = "http://files.vagrantup.com/precise64.box"

  config.vm.define "zookeeper" do |zookeeper|
    zookeeper.vm.network :private_network, ip: "192.168.33.2"
    zookeeper.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    zookeeper.vm.provision "shell", path: "examples/vagrant/provision-zookeeper.sh"
  end

  config.vm.define "mesos-master" do |master|
    master.vm.network :private_network, ip: "192.168.33.3"
    master.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1024"]
    end
    master.vm.provision "shell", path: "examples/vagrant/provision-mesos-master.sh"
  end

  config.vm.define "mesos-slave" do |slave|
    slave.vm.network :private_network, ip: "192.168.33.4"
    slave.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "512"]
    end
    slave.vm.provision "shell", path: "examples/vagrant/provision-mesos-slave.sh"
  end

  config.vm.define "aurora-scheduler" do |scheduler|
    scheduler.vm.network :private_network, ip: "192.168.33.5"
    scheduler.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "1024"]
    end
    scheduler.vm.provision "shell", path: "examples/vagrant/provision-aurora-scheduler.sh"
  end

  config.vm.define "devtools" do |devtools|
    devtools.vm.network :private_network, ip: "192.168.33.6"
    devtools.vm.provider :virtualbox do |vb|
      vb.customize ["modifyvm", :id, "--memory", "2048", "--cpus", "8"]
    end
  end
end
