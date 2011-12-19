package com.twitter.mesos.angrybird;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;

import org.apache.thrift.protocol.TBinaryProtocol;

import com.google.inject.Inject;
import com.google.inject.Module;
import com.twitter.common.application.AbstractApplication;
import com.twitter.common.application.AppLauncher;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.ShutdownRegistry;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.args.constraints.Positive;
import com.twitter.common.base.Command;
import com.twitter.finagle.builder.Server;
import com.twitter.finagle.builder.ServerBuilder;
import com.twitter.finagle.thrift.ThriftServerFramedCodec;
import com.twitter.mesos.angrybird.gen.ZooKeeperThriftServer;
import com.twitter.util.Duration;

public class AngryBirdZooKeeperMain extends AbstractApplication {

  private static final Logger LOG = Logger.getLogger(AngryBirdZooKeeperMain.class.getName());

  @NotNull
  @Positive
  @CmdLine(name="thrift_port", help= "Thrift server port.")
  private static final Arg<Integer> THRIFT_PORT = Arg.create();

  @Nonnegative
  @CmdLine(name="zk_port", help = "Zookeeper server port")
  private static final Arg<Integer> ZK_PORT = Arg.create(0);

  private AngryBirdZooKeeperServer zooKeeperServer;

  @Inject private Lifecycle lifecycle;

  @Inject private ShutdownRegistry shutdownRegistry;

  @Override
  public void run() {
    LOG.log(Level.INFO, "Starting AngryBird ZooKeeper Server");

    try {
      zooKeeperServer = new AngryBirdZooKeeperServer(ZK_PORT.get(), shutdownRegistry);

      int port = zooKeeperServer.startNetwork();

      startThriftServer(THRIFT_PORT.get());

      LOG.log(Level.INFO, String.format("ZooKeeper server started on: %d", port));

    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception starting server ", e);
    }

    lifecycle.awaitShutdown();
  }

  private final void startThriftServer(int port) {
    // start the thrift server
    InetSocketAddress address = new InetSocketAddress(port);

    final Server thriftServer = ServerBuilder.safeBuild(
        new ZooKeeperThriftServer.Service(new AngryBirdZooKeeperThriftService(zooKeeperServer),
            new TBinaryProtocol.Factory()),
        ServerBuilder.get()
            .name("AngryBirdZooKeeperServer")
            .codec(ThriftServerFramedCodec.get())
            .bindTo(address));

    shutdownRegistry.addAction(new Command() {
      @Override public void execute() {
        thriftServer.close(Duration.forever());
      }
    });
  }

  @Override
  public Iterable<? extends Module> getModules() {
    return Arrays.asList(new LifecycleModule());
  }

  public static void main(String[] args) {
    AppLauncher.launch(AngryBirdZooKeeperMain.class, args);
  }
}
