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
