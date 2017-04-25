# Upgrading Aurora

Aurora can be updated from one version to the next without any downtime or restarts of running
jobs. The same holds true for Mesos.

Generally speaking, Mesos and Aurora strive for a +1/-1 version compatibility, i.e. all components
are meant to be forward and backwards compatible for at least one version. This implies it
does not really matter in which order updates are carried out.

Exceptions to this rule are documented in the [Aurora release-notes](../../RELEASE-NOTES.md)
and the [Mesos upgrade instructions](https://mesos.apache.org/documentation/latest/upgrades/).


## Instructions

To upgrade Aurora, follow these steps:

1. Update the first scheduler instance by updating its software and restarting its process.
2. Wait until the scheduler is up and its [Replicated Log](configuration.md#replicated-log-configuration)
   caught up with the other schedulers in the cluster. The log has caught up if `log/recovered` has
   the value `1`. You can check the metric via `curl LIBPROCESS_IP:LIBPROCESS_PORT/metrics/snapshot`,
   where ip and port refer to the [libmesos configuration](configuration.md#network-configuration)
   settings of the scheduler instance.
3. Proceed with the next scheduler until all instances are updated.
4. Update the Aurora executor deployed to the compute nodes of your cluster. Jobs will continue
   running with the old version of the executor, and will only be launched by the new one once
   they are restarted eventually due to natural cluster churn.
5. Distribute the new Aurora client to your users.


## Best Practices

Even though not absolutely mandatory, we advice to adhere to the following rules:

* Never skip any major or minor releases when updating. If you have to catch up several releases you
  have to deploy all intermediary versions. Skipping bugfix releases is acceptable though.
* Verify all updates on a test cluster before touching your production deployments.
* To minimize the number of failovers during updates, update the currently leading scheduler
  instance last.
* Update the Aurora executor on a subset of compute nodes as a canary before deploying the change to
  the whole fleet.
