package com.twitter.mesos.scheduler.log.mesos;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import org.apache.mesos.Log;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.net.InetSocketAddressHelper;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ZooKeeperClient.Credentials;
import com.twitter.common_internal.zookeeper.ZooKeeper;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.storage.LogEntry;

/**
 * Binds a native mesos Log implementation.
 *
 * <p>Requires the following bindings:
 * <ul>
 *   <li>{@link Credentials} - zk authentication credentials</li>
 *   <li>{@literal @ZooKeeper} List&lt;InetSocketAddress&gt; - zk cluster addresses</li>
 *   <li>{@literal @ZooKeeper} Amount&lt;Integer, Time&gt; - zk session timeout to use</li>
 * </ul>
 *
 * <p>Exports the following bindings:
 * <ul>
 *   <li>{@link Log} - a log backed by the mesos native distributed log</li>
 * </ul>
 *
 * @author John Sirois
 */
public class MesosLogStreamModule extends PrivateModule {
  @CmdLine(name = "native_log_quorum_size",
           help = "The size of the quorum required for all log mutations.")
  private static final Arg<Integer> quorumSize = Arg.create(1);

  @NotNull
  @CmdLine(name = "native_log_file_path",
           help = "Path to a file to store the native log data in.  If the parent directory does" +
                  "not exist it will be created.")
  private static final Arg<File> logPath = Arg.create();

  @CmdLine(name = "native_log_zk_group_path",
           help = "A zookeeper node for use by the native log to track the master coordinator.")
  private static final Arg<String> zkLogGroupPath = Arg.create();

  /**
   * Binds a distributed {@link com.twitter.mesos.scheduler.log.Log} that uses the mesos core native
   * log implementation.
   *
   * @param binder a guice binder to bind the distributed log with
   */
  public static void bind(Binder binder) {
    binder.install(new MesosLogStreamModule());
  }

  @Override
  protected void configure() {
    requireBinding(Credentials.class);
    requireBinding(Key.get(new TypeLiteral<List<InetSocketAddress>>() {}, ZooKeeper.class));
    requireBinding(Key.get(new TypeLiteral<Amount<Integer, Time>>() {}, ZooKeeper.class));

    bind(com.twitter.mesos.scheduler.log.Log.class).to(MesosLog.class).in(Singleton.class);
    expose(com.twitter.mesos.scheduler.log.Log.class);
  }

  @Provides
  @Singleton
  Log provideLog(@ZooKeeper List<InetSocketAddress> endpoints,
                 Credentials credentials, @ZooKeeper Amount<Integer, Time> sessionTimeout) {

    File logFile = logPath.get();
    File parentDir = logFile.getParentFile();
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      addError("Failed to create parent directory to store native log at: %s", parentDir);
    }

    String zkConnectString =
        Joiner.on(',').join(Iterables.transform(endpoints, InetSocketAddressHelper.INET_TO_STR));

    return new Log(quorumSize.get(),
                   logFile.getAbsolutePath(),
                   zkConnectString,
                   sessionTimeout.getValue(),
                   sessionTimeout.getUnit().getTimeUnit(),
                   zkLogGroupPath.get(),
                   credentials.scheme(),
                   credentials.authToken());
  }

  @Provides
  @Singleton
  @MesosLog.NoopEntry
  byte[] provideNoopEntry() throws ThriftBinaryCodec.CodingException {
    return ThriftBinaryCodec.encodeNonNull(LogEntry.noop(true));
  }
}
