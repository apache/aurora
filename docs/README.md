# Overview

*Aurora* is a service scheduler that schedules jobs onto *Mesos*, which runs tasks at a specified cluster. Typical services consist of up to hundreds of task replicas.

Aurora provides a *Job* abstraction consisting of a *Task* template and instructions for creating near-identical replicas of that Task (modulo things like "instance id" or specific port numbers which may differ from machine to machine).

*Terminology Note*: *Replicas* are also referred to as *shards* and *instances*. While there is a general desire to move to using "instances", "shard" is still found in commands and help strings.

Typically a Task is a single *Process* corresponding to a single command line, such as `python2.6 my_script.py`. However, sometimes you must colocate separate Processes together within a single Task, which runs within a single container and `chroot`, often referred to as a "sandbox". For example, if you run multiple cooperating agents together such as `logrotate`, `installer`, and master or slave processes. *Thermos* provides a Process abstraction under the Mesos Tasks.

To use and get up to speed on Aurora, you should look the docs in this directory in this order:

1. How to [deploy Aurora](deploying-aurora-scheduler.md) or, how to [install Aurora on virtual machines on your private machine](vagrant.md) (the Tutorial uses the virtual machine approach).
2. As a user, get started quickly with a [Tutorial](tutorial.md).
3. For an overview of Aurora's process flow under the hood, see the [User Guide](userguide.md).
4. To learn how to write a configuration file, look at our [Configuration Tutorial](configurationtutorial.md). From there, look at the [Aurora + Thermos Reference](configurationreference.md).
5. Then read up on the [Aurora Command Line Client](clientcommands.md).
6. Find out general information and useful tips about how Aurora does [Resource Isolation](resourceisolation.md).

To contact the Aurora Developer List, email [dev@aurora.incubator.apache.org](mailto:dev@aurora.incubator.apache.org). You may want to read the list [archives](http://mail-archives.apache.org/mod_mbox/incubator-aurora-dev/). You can also use the IRC channel `#aurora` on `irc.freenode.net`
