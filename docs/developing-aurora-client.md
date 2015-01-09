Getting Started
===============

The client is written in Python, and uses the
[Pants](http://pantsbuild.github.io/python-readme.html) build tool.

Client Configuration
====================

The client uses a configuration file that specifies available clusters. More information about the
contents of this file can be found in the
[Client Cluster Configuration](client-cluster-configuration.md) documentation. Information about
how the client locates this file can be found in the
[Client Commands](client-commands.md#cluster-configuration) documentation.

Building and Testing the Client
===============================

Building and testing the client code are both done using Pants. The relevant targets to know about
are:

   * Build a client executable: `./pants src/main/python/apache/aurora/client/cli:aurora`
   * Test client code: `./pants ./pants src/test/python/apache/aurora/client/cli:all`

Running/Debugging the Client
============================

For manually testing client changes against a cluster, we use [Vagrant](https://www.vagrantup.com/).
To start a virtual cluster, you need to install Vagrant, and then run `vagrant up` for the root of
the aurora workspace. This will create a vagrant host named "devcluster", with a mesos master, a set
of mesos slaves, and an aurora scheduler.

If you have changed you would like to test in your local cluster, you'll rebuild the client:

    vagrant ssh -c 'aurorabuild client'

Once this completes, the `aurora` command will reflect your changes.
