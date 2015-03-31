## Introduction
Apache Aurora is a service scheduler that runs on top of Apache Mesos, enabling you to run long-running services that take advantage of Apache Mesos' scalability, fault-tolerance, and resource isolation. This documentation has been organized into sections with three audiences in mind:
 
 * Users: General information about the project and to learn how to run an Aurora job.
 * Operators: For those that wish to manage and fine-tune an Aurora cluster.
 * Developers: All the information you need to start modifying Aurora and contributing back to the project.

We encourage you to ask questions on the [Aurora developer list](http://aurora.apache.org/community/) or the `#aurora` IRC channel on `irc.freenode.net`.

## Users
 * [Install Aurora on virtual machines on your private machine](vagrant.md)
 * [Hello World Tutorial](tutorial.md)
 * [User Guide](user-guide.md)
 * [Configuration Tutorial](configuration-tutorial.md)
 * [Aurora + Thermos Reference](configuration-reference.md)
 * [Command Line Client](client-commands.md)
 * [Cron Jobs](cron-jobs.md)

## Operators
 * [Deploy Aurora](deploying-aurora-scheduler.md)
 * [Monitoring](monitoring.md)
 * [Hooks for Aurora Client API](hooks.md)
 * [Scheduler Storage](storage.md)
 * [Scheduler Storage and Maintenance](storage-config.md)
 * [SLA Measurement](sla.md)
 * [Resource Isolation and Sizing](resource-isolation.md)
 * [Generating test resources](test-resource-generation.md)

## Developers
 * [Contributing to the project](contributing.md)
 * [Developing the Aurora Scheduler](developing-aurora-scheduler.md)
 * [Developing the Aurora Client](developing-aurora-client.md)
 * [Committers Guide](committers.md)
