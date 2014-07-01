Getting Started
===============
To replicate a real cluster environment as closely as possible, we use
[Vagrant](http://www.vagrantup.com/) to launch a complete Aurora cluster in a virtual machine.

Prerequisites
-------------
  * [VirtualBox](https://www.virtualbox.org/)
  * [Vagrant](http://www.vagrantup.com/)
  * A clone of the Aurora repository, or source distribution.

You can start a local cluster by running:

    vagrant up

Once started, several services should be running:

  * scheduler is listening on http://192.168.33.7:8081
  * observer is listening on http://192.168.33.7:1338
  * master is listening on http://192.168.33.7:5050
  * slave is listening on http://192.168.33.7:5051

You can SSH into the machine with `vagrant ssh` and execute aurora client commands using the
`aurora` command.  A pre-installed `clusters.json` file refers to your local cluster as
`devcluster`, which you will use in client commands.

Deleting your local cluster
===========================
Once you are finished with your local cluster, or if you would otherwise like to start from scratch,
you can use the command `vagrant destroy` to turn off and delete the virtual file system.


Rebuilding components
=====================
If you are changing Aurora code and would like to rebuild a component, you can use the `aurorabuild`
command on your vagrant machine to build and restart a component.  This is considerably faster than
destroying and rebuilding your VM.

`aurorabuild` accepts a list of components to build and update.  You may invoke the command with
no arguments to get a list of supported components.

     vagrant ssh -c 'sudo aurorabuild client'


Troubleshooting
===============
Most of the vagrant related problems can be fixed by the following steps:
* Destroying the vagrant environment with `vagrant destroy`
* Killing any orphaned VMs (see AURORA-499) with `virtualbox` UI or `VBoxManage` command line tool
* Cleaning the repository of build artifacts and other intermediate output with `git clean -fdx`
* Bringing up the vagrant environment with `vagrant up`
