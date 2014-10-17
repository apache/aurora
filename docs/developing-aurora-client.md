Getting Started
===============

Aurora consists of four main pieces: the scheduler (which finds resources in the cluster that can be used to run a job), the executor (which uses the resources assigned by the scheduler to run a job), the command-line client, and the web-ui. For information about working on the scheduler or the webUI, see the file "developing-aurora-scheduler.md" in this directory.

If you want to work on the command-line client, this is the place for you!

The client is written in Python, and unlike the server side of things, we build the client using the Pants build tool, instead of Gradle. Pants is a tool that was built by twitter for handling builds of large collaborative systems. You can see a detailed explanation of
pants [here](http://pantsbuild.github.io/python-readme.html).

To build the client executable, run the following in a command-shell:

    $ ./pants src/main/python/apache/aurora/client/cli:aurora2

This will produce a python executable _pex_ file in `dist/aurora2.pex`. Pex files
are fully self-contained executables: just copy the pex file into your path, and you'll be able to run it. For example, for a typical installation:

    $ cp dist/aurora2.pex /usr/local/bin/aurora

To run all of the client tests:

    $ ./pasts src/test/python/apache/aurora/client/:all


Client Configuration
====================

The client uses a configuration file that specifies available clusters. More information about the
contents of this file can be found in the
[Client Cluster Configuration](client-cluster-configuration.md) documentation. Information about
how the client locates this file can be found in the
[Client Commands](client-commands.md#cluster-configuration) documentation.

Client Versions
===============

There are currently two versions of the aurora client, imaginatively known as v1 and v2. All new development is done entirely in v2, but we continue to support and fix bugs in v1, until we get to the point where v2 is feature-complete and tested, and aurora users have had some time at adapt and switch their processes to use v2.

Both versions are built on the same underlying API code.

Client v1 was implemented using twitter.common.app. The command-line processing code for v1 can be found in `src/main/python/apache/aurora/client/commands` and
`src/main/python/apache/aurora/client/bin`.

Client v2 was implemented using its own noun/verb framework. The client v2 code can be found in `src/main/python/apache/aurora/client/cli`, and the noun/verb framework can be
found in the `__init__.py` file in that directory.


Building and Testing the Client
===============================

Building and testing the client code are both done using Pants. The relevant targets to know about are:

   * Build a client v2 executable: `./pants src/main/python/apache/aurora/client/cli:aurora2`
   * Test client v2 code: `./pants ./pants src/test/python/apache/aurora/client/cli:all`
   * Build a client v1 executable: `./pants src/main/python/apache/aurora/client/bin:aurora_client`
   * Test client v1 code: `./pants src/main/python/apache/aurora/client/commands:all`
   * Test all client code: `./pants src/main/python/apache/aurora/client:all`


Overview of the Client Architecture
===================================

The client is built on a stacked architecture:

   1. At the lowest level, we have a thrift RPC API interface
    to the aurora scheduler. The interface is declared in thrift, in the file
    `src/main/thrift/org/apache/aurora/gen/api.thrift`.

  2. On top of the primitive API, we have a client API. The client API
    takes the primitive operations provided by the scheduler, and uses them
    to implement client-side behaviors. For example, when you update a job,
    on the scheduler, that's done by a sequence of operations.  The sequence is implemented
    by the client API `update` method, which does the following using the thrift API:
     * fetching the state of task instances in the mesos cluster, and figuring out which need
       to be updated;
     * For each task to be updated:
         - killing the old version;
         - starting the new version;
         - monitoring the new version to ensure that the update succeeded.
  3. On top of the API, we have the command-line client itself. The core client, at this level,
    consists of the interface to the command-line which the user will use to interact with aurora.
    The client v2 code is found in `src/python/apache/aurora/client/cli`. In the `cli` directory,
    the rough structure is as follows:
       * `__init__.py` contains the noun/verb command-line processing framework used by client v2.
       * `jobs.py` contains the implementation of the core `job` noun, and all of its operations.
       * `bridge.py` contains the implementation of a component that allows us to ship a
         combined client that runs both v1 and v2 client commands during the transition period.
       * `client.py` contains the code that binds the client v2 nouns and verbs into an executable.

Running/Debugging the Client
============================

For manually testing client changes against a cluster, we use vagrant. To start a virtual cluster,
you need to install a working vagrant environment, and then run "vagrant up" for the root of
the aurora workspace. This will create a vagrant host named "devcluster", with a mesos master,
a set of mesos slaves, and an aurora scheduler.

To use the devcluster, you need to bring it up by running `vagrant up`, and then connect to the vagrant host using `vagrant ssh`. This will open a bash session on the virtual machine hosting the devcluster. In the home directory, there are two key paths to know about:

   * `~/aurora`: this is a copy of the git workspace in which you launched the vagrant cluster.
     To test client changes, you'll use this copy.
   * `/vagrant`: this is a mounted filesystem that's a direct image of your git workspace.
     This isn't a copy - it is your git workspace. Editing files on your host machine will
     be immediately visible here, because they are the same files.

Whenever the scheduler is modified, to update your vagrant environment to use the new scheduler,
you'll need to re-initialize your vagrant images. To do this, you need to run two commands:

   * `vagrant destroy`: this will delete the old devcluster image.
   * `vagrant up`: this creates a fresh devcluster image based on the current state of your workspace.

You should try to minimize rebuilding vagrant images; it's not horribly slow, but it does take a while.

To test client changes:

   * Make a change in your local workspace, and commit it.
   * `vagrant ssh` into the devcluster.
   * `cd aurora`
   * Pull your changes into the vagrant copy: `git pull /vagrant *branchname*`.
   * Build the modified client using pants.
   * Run your command using `aurora2`. (You don't need to do any install; the aurora2 command
     is a symbolic link to the executable generated by pants.)
