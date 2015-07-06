Getting Started
===============

This document shows you how to configure a complete cluster using a virtual machine. This setup
replicates a real cluster in your development machine as closely as possible. After you complete
the steps outlined here, you will be ready to create and run your first Aurora job.

The following sections describe these steps in detail:

1. [Overview](#user-content-overview)
1. [Install VirtualBox and Vagrant](#user-content-install-virtualbox-and-vagrant)
1. [Clone the Aurora repository](#user-content-clone-the-aurora-repository)
1. [Start the local cluster](#user-content-start-the-local-cluster)
1. [Log onto the VM](#user-content-log-onto-the-vm)
1. [Run your first job](#user-content-run-your-first-job)
1. [Rebuild components](#user-content-rebuild-components)
1. [Shut down or delete your local cluster](#user-content-shut-down-or-delete-your-local-cluster)
1. [Troubleshooting](#user-content-troubleshooting)


Overview
--------

The Aurora distribution includes a set of scripts that enable you to create a local cluster in
your development machine. These scripts use [Vagrant](https://www.vagrantup.com/) and
[VirtualBox](https://www.virtualbox.org/) to run and configure a virtual machine. Once the
virtual machine is running, the scripts install and initialize Aurora and any required components
to create the local cluster.


Install VirtualBox and Vagrant
------------------------------

First, download and install [VirtualBox](https://www.virtualbox.org/) on your development machine.

Then download and install [Vagrant](https://www.vagrantup.com/). To verify that the installation
was successful, open a terminal window and type the `vagrant` command. You should see a list of
common commands for this tool.


Clone the Aurora repository
---------------------------

To obtain the Aurora source distribution, clone its Git repository using the following command:

     git clone git://git.apache.org/aurora.git


Start the local cluster
-----------------------

Now change into the `aurora/` directory, which contains the Aurora source code and
other scripts and tools:

     cd aurora/

To start the local cluster, type the following command:

     vagrant up

This command uses the configuration scripts in the Aurora distribution to:

* Download a Linux system image.
* Start a virtual machine (VM) and configure it.
* Install the required build tools on the VM.
* Install Aurora's requirements (like [Mesos](http://mesos.apache.org/) and
[Zookeeper](http://zookeeper.apache.org/)) on the VM.
* Build and install Aurora from source on the VM.
* Start Aurora's services on the VM.

This process takes several minutes to complete.

To verify that Aurora is running on the cluster, visit the following URLs:

* Scheduler - http://192.168.33.7:8081
* Observer - http://192.168.33.7:1338
* Mesos Master - http://192.168.33.7:5050
* Mesos Slave - http://192.168.33.7:5051


Log onto the VM
---------------

To SSH into the VM, run the following command in your development machine:

     vagrant ssh

To verify that Aurora is installed in the VM, type the `aurora` command. You should see a list
of arguments and possible commands.

The `/vagrant` directory on the VM is mapped to the `aurora/` local directory
from which you started the cluster. You can edit files inside this directory in your development
machine and access them from the VM under `/vagrant`.

A pre-installed `clusters.json` file refers to your local cluster as `devcluster`, which you
will use in client commands.


Run your first job
------------------

Now that your cluster is up and running, you are ready to define and run your first job in Aurora.
For more information, see the [Aurora Tutorial](tutorial.md).


Rebuild components
------------------

If you are changing Aurora code and would like to rebuild a component, you can use the `aurorabuild`
command on the VM to build and restart a component.  This is considerably faster than destroying
and rebuilding your VM.

`aurorabuild` accepts a list of components to build and update. To get a list of supported
components, invoke the `aurorabuild` command with no arguments:

     vagrant ssh -c 'aurorabuild client'


Shut down or delete your local cluster
--------------------------------------

To shut down your local cluster, run the `vagrant halt` command in your development machine. To
start it again, run the `vagrant up` command.

Once you are finished with your local cluster, or if you would otherwise like to start from scratch,
you can use the command `vagrant destroy` to turn off and delete the virtual file system.


Troubleshooting
---------------

Most of the vagrant related problems can be fixed by the following steps:

* Destroying the vagrant environment with `vagrant destroy`
* Killing any orphaned VMs (see AURORA-499) with `virtualbox` UI or `VBoxManage` command line tool
* Cleaning the repository of build artifacts and other intermediate output with `git clean -fdx`
* Bringing up the vagrant environment with `vagrant up`
