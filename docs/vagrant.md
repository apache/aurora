Aurora includes a `Vagrantfile` that defines a full Mesos cluster running Aurora. You can use it to
explore Aurora's various components. To get started, install
[VirtualBox](https://www.virtualbox.org/) and [Vagrant](http://www.vagrantup.com/),
then run `vagrant up` somewhere in the repository source tree to create a team of VMs.  This may take some time initially as it builds all
the components involved in running an aurora cluster.

The scheduler is listening on http://192.168.33.7:8081/scheduler
The observer is listening on http://192.168.33.7:1338
The master is listening on http://192.168.33.7:5050

Once everything is up, you can `vagrant ssh devcluster` and execute aurora client commands using the `aurora` client.

Troubleshooting
---------------
Most of the vagrant related problems can be fixed by the following steps:
* Destroying the vagrant environment with `vagrant destroy`
* Cleaning the repository of build artifacts and other intermediate output with `git clean -fdx`
* Bringing up the vagrant environment with `vagrant up`
