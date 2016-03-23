# Installing Aurora

- [Components](#components)
    - [Machine profiles](#machine-profiles)
      - [Coordinator](#coordinator)
      - [Worker](#worker)
      - [Client](#client)
- [Getting Aurora](#getting-aurora)
    - [Building your own binary packages](#building-your-own-binary-packages)
    - [RPMs](#rpms)
- [Installing the scheduler](#installing-the-scheduler)
    - [Ubuntu Trusty](#ubuntu-trusty)
    - [CentOS 7](#centos-7)
    - [Finalizing](#finalizing)
    - [Configuration](#configuration)
- [Installing worker components](#installing-worker-components)
    - [Ubuntu Trusty](#ubuntu-trusty-1)
    - [CentOS 7](#centos-7-1)
    - [Configuration](#configuration-1)
- [Installing the client](#installing-the-client)
    - [Ubuntu Trusty](#ubuntu-trusty-2)
    - [CentOS 7](#centos-7-2)
    - [Configuration](#configuration-2)
- [See also](#see-also)
- [Installing Mesos](#installing-mesos)
    - [Mesos on Ubuntu Trusty](#mesos-on-ubuntu-trusty)
    - [Mesos on CentOS 7](#mesos-on-centos-7)

## Components
Before installing Aurora, it's important to have an understanding of the components that make up
a functioning Aurora cluster.

![Aurora Components](images/components.png)

* **Aurora scheduler**  
  The scheduler will be your primary interface to the work you run in your cluster.  You will
  instruct it to run jobs, and it will manage them in Mesos for you.  You will also frequently use
  the scheduler's web interface as a heads-up display for what's running in your cluster.

* **Aurora client**  
  The client (`aurora` command) is a command line tool that exposes primitives that you can use to
  interact with the scheduler.

  Aurora also provides an admin client (`aurora_admin` command) that contains commands built for
  cluster administrators.  You can use this tool to do things like manage user quotas and manage
  graceful maintenance on machines in cluster.

* **Aurora executor**  
  The executor (a.k.a. Thermos executor) is responsible for carrying out the workloads described in
  the Aurora DSL (`.aurora` files).  The executor is what actually executes user processes.  It will
  also perform health checking of tasks and register tasks in ZooKeeper for the purposes of dynamic
  service discovery.  You can find lots more detail on the executor and Thermos in the
  [user guide](user-guide.md).

* **Aurora observer**  
  The observer provides browser-based access to the status of individual tasks executing on worker
  machines.  It gives insight into the processes executing, and facilitates browsing of task sandbox
  directories.

* **ZooKeeper**  
  [ZooKeeper](http://zookeeper.apache.org) is a distributed consensus system.  In an Aurora cluster
  it is used for reliable election of the leading Aurora scheduler and Mesos master.

* **Mesos master**  
  The master is responsible for tracking worker machines and performing accounting of their
  resources.  The scheduler interfaces with the master to control the cluster.

* **Mesos agent**  
  The agent receives work assigned by the scheduler and executes them.  It interfaces with Linux
  isolation systems like cgroups, namespaces and Docker to manage the resource consumption of tasks.
  When a user task is launched, the agent will launch the executor (in the context of a Linux cgroup
  or Docker container depending upon the environment), which will in turn fork user processes.

## Machine profiles
Given that many of these components communicate over the network, there are numerous ways you could
assemble them to create an Aurora cluster.  The simplest way is to think in terms of three machine
profiles:

### Coordinator
**Components**: ZooKeeper, Aurora scheduler, Mesos master

A small number of machines (typically 3 or 5) responsible for cluster orchestration.  In most cases
it is fine to co-locate these components in anything but very large clusters (> 1000 machines).
Beyond that point, operators will likely want to manage these services on separate machines.

In practice, 5 coordinators have been shown to reliably manage clusters with tens of thousands of
machines.


### Worker
**Components**: Aurora executor, Aurora observer, Mesos agent

The bulk of the cluster, where services will actually run.

### Client
**Components**: Aurora client, Aurora admin client

Any machines that users submit jobs from.

## Getting Aurora
Source and binary distributions can be found on our
[downloads](https://aurora.apache.org/downloads/) page.  Installing from binary packages is
recommended for most.

### Building your own binary packages
Our package build toolchain makes it easy to build your own packages if you would like.  See the
[instructions](https://github.com/apache/aurora-packaging) to learn how.

## Installing the scheduler
### Ubuntu Trusty

1. Install Mesos  
   Skip down to [install mesos](#mesos-on-ubuntu-trusty), then run:

        sudo start mesos-master

2. Install ZooKeeper

        sudo apt-get install -y zookeeperd

3. Install the Aurora scheduler

        sudo add-apt-repository -y ppa:openjdk-r/ppa
        sudo apt-get update
        sudo apt-get install -y openjdk-8-jre-headless wget

        sudo update-alternatives --set java /usr/lib/jvm/java-8-openjdk-amd64/jre/bin/java

        wget -c https://apache.bintray.com/aurora/ubuntu-trusty/aurora-scheduler_0.12.0_amd64.deb
        sudo dpkg -i aurora-scheduler_0.12.0_amd64.deb

### CentOS 7

1. Install Mesos  
   Skip down to [install mesos](#mesos-on-centos-7), then run:

        sudo systemctl start mesos-master

2. Install ZooKeeper

        sudo rpm -Uvh https://archive.cloudera.com/cdh4/one-click-install/redhat/6/x86_64/cloudera-cdh-4-0.x86_64.rpm
        sudo yum install -y java-1.8.0-openjdk-headless zookeeper-server

        sudo service zookeeper-server init
        sudo systemctl start zookeeper-server

3. Install the Aurora scheduler

        sudo yum install -y wget

        wget -c https://apache.bintray.com/aurora/centos-7/aurora-scheduler-0.12.0-1.el7.centos.aurora.x86_64.rpm
        sudo yum install -y aurora-scheduler-0.12.0-1.el7.centos.aurora.x86_64.rpm

### Finalizing
By default, the scheduler will start in an uninitialized mode.  This is because external
coordination is necessary to be certain operator error does not result in a quorum of schedulers
starting up and believing their databases are empty when in fact they should be re-joining a
cluster.

Because of this, a fresh install of the scheduler will need intervention to start up.  First,
stop the scheduler service.  
Ubuntu: `sudo stop aurora-scheduler`  
CentOS: `sudo systemctl stop aurora`

Now initialize the database:

    sudo -u aurora mkdir -p /var/lib/aurora/scheduler/db
    sudo -u aurora mesos-log initialize --path=/var/lib/aurora/scheduler/db

Now you can start the scheduler back up.  
Ubuntu: `sudo start aurora-scheduler`  
CentOS: `sudo systemctl start aurora`

### Configuration
For more detail on this topic, see the dedicated page on
[deploying the scheduler](deploying-aurora-scheduler.md)


## Installing worker components
### Ubuntu Trusty

1. Install Mesos  
   Skip down to [install mesos](#mesos-on-ubuntu-trusty), then run:

        start mesos-slave

2. Install Aurora executor and observer

        sudo apt-get install -y python2.7 wget

        # NOTE: This appears to be a missing dependency of the mesos deb package and is needed
        # for the python mesos native bindings.
        sudo apt-get -y install libcurl4-nss-dev

        wget -c https://apache.bintray.com/aurora/ubuntu-trusty/aurora-executor_0.12.0_amd64.deb
        sudo dpkg -i aurora-executor_0.12.0_amd64.deb

### CentOS 7

1. Install Mesos  
   Skip down to [install mesos](#mesos-on-centos-7), then run:

        sudo systemctl start mesos-slave

2. Install Aurora executor and observer

        sudo yum install -y python2 wget

        wget -c https://apache.bintray.com/aurora/centos-7/aurora-executor-0.12.0-1.el7.centos.aurora.x86_64.rpm
        sudo yum install -y aurora-executor-0.12.0-1.el7.centos.aurora.x86_64.rpm

### Configuration
The executor typically does not require configuration.  Command line arguments can
be passed to the executor using a command line argument on the scheduler.

The observer needs to be configured to look at the correct mesos directory in order to find task
sandboxes. You should 1st find the Mesos working directory by looking for the Mesos slave
`--work_dir` flag. You should see something like:

        ps -eocmd | grep "mesos-slave" | grep -v grep | tr ' ' '\n' | grep "\--work_dir"
        --work_dir=/var/lib/mesos

If the flag is not set, you can view the default value like so:

        mesos-slave --help
        Usage: mesos-slave [options]

          ...
          --work_dir=VALUE      Directory path to place framework work directories
                                (default: /tmp/mesos)
          ...

The value you find for `--work_dir`, `/var/lib/mesos` in this example, should match the Aurora
observer value for `--mesos-root`.  You can look for that setting in a similar way on a worker
node by grepping for `thermos_observer` and `--mesos-root`.  If the flag is not set, you can view
the default value like so:

        thermos_observer -h
        Options:
          ...
          --mesos-root=MESOS_ROOT
                                The mesos root directory to search for Thermos
                                executor sandboxes [default: /var/lib/mesos]
          ...

In this case the default is `/var/lib/mesos` and we have a match. If there is no match, you can
either adjust the mesos-master start script(s) and restart the master(s) or else adjust the
Aurora observer start scripts and restart the observers.  To adjust the Aurora observer:

#### Ubuntu Trusty

    sudo sh -c 'echo "MESOS_ROOT=/tmp/mesos" >> /etc/default/thermos'

NB: In Aurora releases up through 0.12.0, you'll also need to edit /etc/init/thermos.conf like so:

    diff -C 1 /etc/init/thermos.conf.orig /etc/init/thermos.conf
    *** /etc/init/thermos.conf.orig 2016-03-22 22:34:46.286199718 +0000
    --- /etc/init/thermos.conf  2016-03-22 17:09:49.357689038 +0000
    ***************
    *** 24,25 ****
    --- 24,26 ----
          --port=${OBSERVER_PORT:-1338} \
    +     --mesos-root=${MESOS_ROOT:-/var/lib/mesos} \
          --log_to_disk=NONE \

#### CentOS 7

Make an edit to add the `--mesos-root` flag resulting in something like:

    grep -A5 OBSERVER_ARGS /etc/sysconfig/thermos-observer
    OBSERVER_ARGS=(
      --port=1338
      --mesos-root=/tmp/mesos
      --log_to_disk=NONE
      --log_to_stderr=google:INFO
    )

## Installing the client
### Ubuntu Trusty

    sudo apt-get install -y python2.7 wget

    wget -c https://apache.bintray.com/aurora/ubuntu-trusty/aurora-tools_0.12.0_amd64.deb
    sudo dpkg -i aurora-tools_0.12.0_amd64.deb

### CentOS 7

    sudo yum install -y python2 wget

    wget -c https://apache.bintray.com/aurora/centos-7/aurora-tools-0.12.0-1.el7.centos.aurora.x86_64.rpm
    sudo yum install -y aurora-tools-0.12.0-1.el7.centos.aurora.x86_64.rpm

### Mac OS X

    brew upgrade
    brew install aurora-cli

### Configuration
Client configuration lives in a json file that describes the clusters available and how to reach
them.  By default this file is at `/etc/aurora/clusters.json`.

Jobs may be submitted to the scheduler using the client, and are described with
[job configurations](configuration-reference.md) expressed in `.aurora` files.  Typically you will
maintain a single job configuration file to describe one or more deployment environments (e.g.
dev, test, prod) for a production job.

## See also
We have other docs that you will find useful once you have your cluster up and running:

- [Monitor](monitoring.md) your cluster
- Enable scheduler [security](security.md)
- View job SLA [statistics](sla.md)
- Understand the internals of the scheduler's [storage](storage.md)

## Installing Mesos
Mesos uses a single package for the Mesos master and slave.  As a result, the package dependencies
are identical for both.

### Mesos on Ubuntu Trusty

    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
    DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
    CODENAME=$(lsb_release -cs)

    echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | \
      sudo tee /etc/apt/sources.list.d/mesosphere.list
    sudo apt-get -y update

    # Use `apt-cache showpkg mesos | grep [version]` to find the exact version.
    sudo apt-get -y install mesos=0.25.0-0.2.70.ubuntu1404

### Mesos on CentOS 7

    sudo rpm -Uvh https://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm
    sudo yum -y install mesos-0.25.0
