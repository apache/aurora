Aurora includes a `Vagrantfile` that defines a full Mesos cluster running Aurora. You can use it to
explore Aurora's various components. To get started, install
[VirtualBox](https://www.virtualbox.org/) and [Vagrant](http://www.vagrantup.com/),
then run `vagrant up` somewhere in the repository source tree to create a team of VMs.  This may take some time initially as it builds all
the components involved in running an aurora cluster.

The scheduler is listening on http://192.168.33.5:8081/scheduler
The observer is listening on http://192.168.33.4:1338/
The master is listening on http://192.168.33.3:5050/

Once everything is up, you can `vagrant ssh aurora-scheduler` and execute aurora client commands using the `aurora` client.
