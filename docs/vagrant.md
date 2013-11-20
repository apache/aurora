Aurora includes a `Vagrantfile` that defines a full Mesos cluster running Aurora. You can use it to
explore Aurora's various components. To get started, install
[VirtualBox](https://www.virtualbox.org/) and [Vagrant](http://www.vagrantup.com/), then run `vagrant up` somewhere in the repository source tree to create a team of VMs.

Creating Aurora Binaries and Serving them to Vagrant
----------------------------------------------------
Run the following commands:

    ./pants src/main/python/twitter/aurora/client/bin:aurora_admin
    ./pants src/main/python/twitter/aurora/client/bin:aurora_client
    ./pants src/main/python/twitter/aurora/executor/bin:gc_executor
    ./pants src/main/python/twitter/aurora/executor/bin:thermos_executor
    ./pants src/main/python/twitter/thermos/observer/bin:thermos_observer
    ./gradlew distTar
    vagrant up

The scheduler is listening on http://192.168.33.5:8081/scheduler
The observer is listening on http://192.168.33.4:1338
The master is listening on http://192.168.33.3:5050/
