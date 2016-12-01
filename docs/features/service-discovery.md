Service Discovery
=================

It is possible for the Aurora executor to announce tasks into ServerSets for
the purpose of service discovery.  ServerSets use the Zookeeper [group membership pattern](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_outOfTheBox)
of which there are several reference implementations:

  - [C++](https://github.com/apache/mesos/blob/master/src/zookeeper/group.cpp)
  - [Java](https://github.com/twitter/commons/blob/master/src/java/com/twitter/common/zookeeper/ServerSetImpl.java#L221)
  - [Python](https://github.com/twitter/commons/blob/master/src/python/twitter/common/zookeeper/serverset/serverset.py#L51)

These can also be used natively in Finagle using the [ZookeeperServerSetCluster](https://github.com/twitter/finagle/blob/master/finagle-serversets/src/main/scala/com/twitter/finagle/zookeeper/ZookeeperServerSetCluster.scala).

For more information about how to configure announcing, see the [Configuration Reference](../reference/configuration.md).

Using Mesos DiscoveryInfo
-------------------------
Experimental support for populating DiscoveryInfo in Mesos is introduced in Aurora. This can be used to build
custom service discovery system not using zookeeper. Please see `Service Discovery` section in
[Mesos Framework Development guide](http://mesos.apache.org/documentation/latest/app-framework-development-guide/) for
explanation of the protobuf message in Mesos.

To use this feature, please enable `--populate_discovery_info` flag on scheduler. All jobs started by scheduler
afterwards will have their portmap populated to Mesos and discoverable in `/state` endpoint in Mesos master and agent.

### Using Mesos DNS
An example is using [Mesos-DNS](https://github.com/mesosphere/mesos-dns), which is able to generate multiple DNS
records. With current implementation, the example job with key `devcluster/vagrant/test/http-example` generates at
least the following:

1. An A record for `http_example.test.vagrant.aurora.mesos` (which only includes IP address);
2. A [SRV record](https://en.wikipedia.org/wiki/SRV_record) for
 `_http_example.test.vagrant._tcp.aurora.mesos`, which includes IP address and every port. This should only
  be used if the service has one port.
3. A SRV record `_{port-name}._http_example.test.vagrant._tcp.aurora.mesos` for each port name
  defined. This should be used when the service has multiple ports.

Things to note:

1. The domain part (".mesos" in above example) can be configured in [Mesos DNS](http://mesosphere.github.io/mesos-dns/docs/configuration-parameters.html);
2. Right now, portmap and port aliases in announcer object are not reflected in DiscoveryInfo, therefore not visible in
   Mesos DNS records either. This is because they are only resolved in thermos executors.
