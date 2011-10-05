package com.twitter.mesos.scheduler.zk;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.logging.Logger;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.collections.Pair;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.ZooKeeperClient.Credentials;
import com.twitter.common.zookeeper.ZooKeeperUtils;
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer;
import com.twitter.common_internal.zookeeper.TwitterZk;

/**
 * Binds ZooKeeper connection information as well as a client.
 *
 * <p>Exports the following bindings:
 * <ul>
 *   <li>{@link Credentials} - zk authentication credentials</li>
 *   <li>{@literal @ZooKeeper} List&lt;InetSocketAddress&gt; - zk cluster addresses</li>
 *   <li>{@literal @ZooKeeper} Amount&lt;Integer, Time&gt; - zk session timeout to use</li>
 *   <li>{@link ZooKeeperClient} - a client connected with the connection info above</li>
 * </ul>
 *
 * @author John Sirois
 */
public class ZooKeeperModule extends PrivateModule {
  private static final Logger LOG = Logger.getLogger(ZooKeeperModule.class.getName());

  @CmdLine(name = "zk_in_proc",
           help ="Launches an embedded zookeeper server for local testing")
  private static final Arg<Boolean> zooKeeperInProcess = Arg.create(false);

  @CmdLine(name = "zk_endpoints", help ="Endpoint specification for the ZooKeeper servers.")
  private static final Arg<List<InetSocketAddress>> zooKeeperEndpoints =
      Arg.<List<InetSocketAddress>>create(ImmutableList.copyOf(TwitterZk.DEFAULT_ZK_ENDPOINTS));

  @CmdLine(name = "zk_session_timeout", help ="The ZooKeeper session timeout.")
  public static final Arg<Amount<Integer, Time>> zooKeeperSessionTimeout =
      Arg.create(ZooKeeperUtils.DEFAULT_ZK_SESSION_TIMEOUT);


  public static void bind(Binder binder) {
    binder.install(new ZooKeeperModule());
  }

  @Override
  protected void configure() {
    // TODO(John Sirois): get this from /etc/keys/mesos:mesos
    bind(Credentials.class).toInstance(ZooKeeperClient.digestCredentials("mesos", "mesos"));

    expose(Credentials.class);
    expose(Key.get(new TypeLiteral<List<InetSocketAddress>>() {}, ZooKeeper.class));
    expose(Key.get(new TypeLiteral<Amount<Integer, Time>>() {}, ZooKeeper.class));
    expose(ZooKeeperClient.class);
  }

  @Provides
  @Singleton
  @ZooKeeper
  Amount<Integer, Time> provideZookeeperSessionTimeout() {
    return zooKeeperSessionTimeout.get();
  }

  @Provides
  @Singleton
  @ZooKeeper
  List<InetSocketAddress> provideZooKeeperEndpoints(
      Pair<? extends List<InetSocketAddress>, ZooKeeperClient> zooKeeperBundle) {
    return zooKeeperBundle.getFirst();
  }

  @Provides
  @Singleton
  ZooKeeperClient provideZooKeeperClient(
      Pair<? extends List<InetSocketAddress>, ZooKeeperClient> zooKeeperBundle) {
    return zooKeeperBundle.getSecond();
  }

  @Provides
  @Singleton
  Pair<? extends List<InetSocketAddress>, ZooKeeperClient> provideZooKeeperClient(
      @ShutdownStage ActionRegistry shutdownRegistry, Credentials credentials) {

    if (zooKeeperInProcess.get()) {
      try {
        return startLocalZookeeper(shutdownRegistry, credentials);
      } catch (IOException e) {
        throw new RuntimeException("Unable to start local zookeeper", e);
      } catch (InterruptedException e) {
        throw new RuntimeException("Unable to start local zookeeper", e);
      }
    } else {
      return Pair.of(zooKeeperEndpoints.get(),
                     new ZooKeeperClient(zooKeeperSessionTimeout.get(), credentials,
                                         zooKeeperEndpoints.get()));
    }
  }

  private Pair<? extends List<InetSocketAddress>, ZooKeeperClient> startLocalZookeeper(
      ActionRegistry shutdownRegistry, ZooKeeperClient.Credentials credentials)
      throws IOException, InterruptedException {

    ZooKeeperTestServer zooKeeperServer = new ZooKeeperTestServer(0, shutdownRegistry);
    zooKeeperServer.startNetwork();
    LOG.info("Embedded zookeeper cluster started on port " + zooKeeperServer.getPort());

    InetSocketAddress localZookeeper =
        InetSocketAddress.createUnresolved("localhost", zooKeeperServer.getPort());
    return Pair.of(ImmutableList.of(localZookeeper),
                   zooKeeperServer.createClient(zooKeeperSessionTimeout.get(), credentials));
  }
}
