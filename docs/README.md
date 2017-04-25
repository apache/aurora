## Introduction

Apache Aurora is a service scheduler that runs on top of Apache Mesos, enabling you to run
long-running services, cron jobs, and ad-hoc jobs that take advantage of Apache Mesos' scalability,
fault-tolerance, and resource isolation.

We encourage you to ask questions on the [Aurora user list](http://aurora.apache.org/community/) or
the `#aurora` IRC channel on `irc.freenode.net`.


## Getting Started
Information for everyone new to Apache Aurora.

 * [Aurora System Overview](getting-started/overview.md)
 * [Hello World Tutorial](getting-started/tutorial.md)
 * [Local cluster with Vagrant](getting-started/vagrant.md)

## Features
Description of important Aurora features.

 * [Containers](features/containers.md)
 * [Cron Jobs](features/cron-jobs.md)
 * [Custom Executors](features/custom-executors.md)
 * [Job Updates](features/job-updates.md)
 * [Multitenancy](features/multitenancy.md)
 * [Resource Isolation](features/resource-isolation.md)
 * [Scheduling Constraints](features/constraints.md)
 * [Services](features/services.md)
 * [Service Discovery](features/service-discovery.md)
 * [SLA Metrics](features/sla-metrics.md)
 * [Webhooks](features/webhooks.md)

## Operators
For those that wish to manage and fine-tune an Aurora cluster.

 * [Installation](operations/installation.md)
 * [Configuration](operations/configuration.md)
 * [Upgrades](operations/upgrades.md)
 * [Troubleshooting](operations/troubleshooting.md)
 * [Monitoring](operations/monitoring.md)
 * [Security](operations/security.md)
 * [Storage](operations/storage.md)
 * [Backup](operations/backup-restore.md)

## Reference
The complete reference of commands, configuration options, and scheduler internals.

 * [Task lifecycle](reference/task-lifecycle.md)
 * Configuration (`.aurora` files)
    - [Configuration Reference](reference/configuration.md)
    - [Configuration Tutorial](reference/configuration-tutorial.md)
    - [Configuration Best Practices](reference/configuration-best-practices.md)
    - [Configuration Templating](reference/configuration-templating.md)
 * Aurora Client
    - [Client Commands](reference/client-commands.md)
    - [Client Hooks](reference/client-hooks.md)
    - [Client Cluster Configuration](reference/client-cluster-configuration.md)
 * [Scheduler Configuration](reference/scheduler-configuration.md)
 * [Observer Configuration](reference/observer-configuration.md)
 * [Endpoints](reference/scheduler-endpoints.md)

## Additional Resources
 * [Tools integrating with Aurora](additional-resources/tools.md)
 * [Presentation videos and slides](additional-resources/presentations.md)

## Developers
All the information you need to start modifying Aurora and contributing back to the project.

 * [Contributing to the project](../CONTRIBUTING.md)
 * [Committer's Guide](development/committers-guide.md)
 * [Design Documents](development/design-documents.md)
 * Developing the Aurora components:
     - [Client](development/client.md)
     - [Scheduler](development/scheduler.md)
     - [Scheduler UI](development/ui.md)
     - [Thermos](development/thermos.md)
     - [Thrift structures](development/thrift.md)


