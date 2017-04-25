# Installing Aurora

Source and binary distributions can be found on our
[downloads](https://aurora.apache.org/downloads/) page.  Installing from binary packages is
recommended for most.

- [Installing the scheduler](#installing-the-scheduler)
- [Installing worker components](#installing-worker-components)
- [Installing the client](#installing-the-client)
- [Installing Mesos](#installing-mesos)
- [Troubleshooting](#troubleshooting)

If our binay packages don't suite you, our package build toolchain makes it easy to build your
own packages. See the [instructions](https://github.com/apache/aurora-packaging) to learn how.


## Machine profiles

Given that many of these components communicate over the network, there are numerous ways you could
assemble them to create an Aurora cluster.  The simplest way is to think in terms of three machine
profiles:

### Coordinator
**Components**: ZooKeeper, Aurora scheduler, Mesos master

A small number of machines (typically 3 or 5) responsible for cluster orchestration.  In most cases
it is fine to co-locate these components in anything but very large clusters (> 1000 machines).
Beyond that point, operators will likely want to manage these services on separate machines.
In particular, you will want to use separate ZooKeeper ensembles for leader election and
service discovery. Otherwise a service discovery error or outage can take down the entire cluster.

In practice, 5 coordinators have been shown to reliably manage clusters with tens of thousands of
machines.

### Worker
**Components**: Aurora executor, Aurora observer, Mesos agent

The bulk of the cluster, where services will actually run.

### Client
**Components**: Aurora client, Aurora admin client

Any machines that users submit jobs from.


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

        wget -c https://apache.bintray.com/aurora/ubuntu-trusty/aurora-scheduler_0.17.0_amd64.deb
        sudo dpkg -i aurora-scheduler_0.17.0_amd64.deb

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

        wget -c https://apache.bintray.com/aurora/centos-7/aurora-scheduler-0.17.0-1.el7.centos.aurora.x86_64.rpm
        sudo yum install -y aurora-scheduler-0.17.0-1.el7.centos.aurora.x86_64.rpm

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

        wget -c https://apache.bintray.com/aurora/ubuntu-trusty/aurora-executor_0.17.0_amd64.deb
        sudo dpkg -i aurora-executor_0.17.0_amd64.deb

### CentOS 7

1. Install Mesos
   Skip down to [install mesos](#mesos-on-centos-7), then run:

        sudo systemctl start mesos-slave

2. Install Aurora executor and observer

        sudo yum install -y python2 wget

        wget -c https://apache.bintray.com/aurora/centos-7/aurora-executor-0.17.0-1.el7.centos.aurora.x86_64.rpm
        sudo yum install -y aurora-executor-0.17.0-1.el7.centos.aurora.x86_64.rpm

### Worker Configuration
The executor typically does not require configuration.  Command line arguments can
be passed to the executor using a command line argument on the scheduler.

The observer needs to be configured to look at the correct mesos directory in order to find task
sandboxes. You should 1st find the Mesos working directory by looking for the Mesos agent
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

#### CentOS 7

Make an edit to add the `--mesos-root` flag resulting in something like:

    grep -A5 OBSERVER_ARGS /etc/sysconfig/thermos
    OBSERVER_ARGS=(
      --port=1338
      --mesos-root=/tmp/mesos
      --log_to_disk=NONE
      --log_to_stderr=google:INFO
    )


## Installing the client
### Ubuntu Trusty

    sudo apt-get install -y python2.7 wget

    wget -c https://apache.bintray.com/aurora/ubuntu-trusty/aurora-tools_0.17.0_amd64.deb
    sudo dpkg -i aurora-tools_0.17.0_amd64.deb

### CentOS 7

    sudo yum install -y python2 wget

    wget -c https://apache.bintray.com/aurora/centos-7/aurora-tools-0.17.0-1.el7.centos.aurora.x86_64.rpm
    sudo yum install -y aurora-tools-0.17.0-1.el7.centos.aurora.x86_64.rpm

### Mac OS X

    brew upgrade
    brew install aurora-cli

### Client Configuration
Client configuration lives in a json file that describes the clusters available and how to reach
them.  By default this file is at `/etc/aurora/clusters.json`.

Jobs may be submitted to the scheduler using the client, and are described with
[job configurations](../reference/configuration.md) expressed in `.aurora` files.  Typically you will
maintain a single job configuration file to describe one or more deployment environments (e.g.
dev, test, prod) for a production job.


## Installing Mesos
Mesos uses a single package for the Mesos master and agent.  As a result, the package dependencies
are identical for both.

### Mesos on Ubuntu Trusty

    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
    DISTRO=$(lsb_release -is | tr '[:upper:]' '[:lower:]')
    CODENAME=$(lsb_release -cs)

    echo "deb http://repos.mesosphere.io/${DISTRO} ${CODENAME} main" | \
      sudo tee /etc/apt/sources.list.d/mesosphere.list
    sudo apt-get -y update

    # Use `apt-cache showpkg mesos | grep [version]` to find the exact version.
    sudo apt-get -y install mesos=1.1.0-2.0.107.ubuntu1404_amd64.deb

### Mesos on CentOS 7

    sudo rpm -Uvh https://repos.mesosphere.io/el/7/noarch/RPMS/mesosphere-el-repo-7-1.noarch.rpm
    sudo yum -y install mesos-1.1.0


## Troubleshooting

So you've started your first cluster and are running into some issues? We've collected some common
stumbling blocks and solutions in our [Troubleshooting guide](troubleshooting.md) to help get you moving.
