package com.twitter.mesos.scheduler.log.mesos;

import java.io.File;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.net.InetSocketAddress;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.BindingAnnotation;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.TypeLiteral;

import org.apache.mesos.Log;
import org.apache.zookeeper.common.PathUtils;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.inject.Bindings;
import com.twitter.common.inject.Bindings.KeyFactory;
import com.twitter.common.inject.Bindings.Rebinder;
import com.twitter.common.net.InetSocketAddressHelper;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.zookeeper.ZooKeeperClient.Credentials;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.gen.storage.LogEntry;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

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
 */
public class MesosLogStreamModule extends PrivateModule {
  @CmdLine(name = "native_log_quorum_size",
           help = "The size of the quorum required for all log mutations.")
  private static final Arg<Integer> QUORUM_SIZE = Arg.create(1);

  @CmdLine(name = "native_log_file_path",
           help = "Path to a file to store the native log data in.  If the parent directory does"
               + "not exist it will be created.")
  private static final Arg<File> LOG_PATH = Arg.create(null);

  @CmdLine(name = "native_log_zk_group_path",
           help = "A zookeeper node for use by the native log to track the master coordinator.")
  private static final Arg<String> ZK_LOG_GROUP_PATH = Arg.create(null);

  /*
   * This timeout includes the time to get a quorum to promise leadership to the coordinator and
   * the time to fill any holes in the coordinator's log.
   */
  @CmdLine(name = "native_log_election_timeout",
           help = "The timeout for a single attempt to obtain a new log writer.")
  private static final Arg<Amount<Long, Time>> COORDINATOR_ELECTION_TIMEOUT =
      Arg.create(Amount.of(15L, Time.SECONDS));

  /*
   * Normally retries would not be expected to help much - however in the small replica set where
   * a few down replicas doom a coordinator election attempt, retrying effectively gives us a wider
   * window in which to await a live quorum before giving up and thrashing the global election
   * process.  Observed log replica recovery times as of 4/6/2012 can be ~45 seconds so giving a
   * window >= 2x this should support 1-round election events (that possibly use several retries in
   * the single round).
   */
  @CmdLine(name = "native_log_election_retries",
           help = "The maximum number of attempts to obtain a new log writer.")
  private static final Arg<Integer> COORDINATOR_ELECTION_RETRIES = Arg.create(20);

  @CmdLine(name = "native_log_read_timeout",
           help = "The timeout for doing log reads.")
  private static final Arg<Amount<Long, Time>> READ_TIMEOUT =
      Arg.create(Amount.of(5L, Time.SECONDS));

  @CmdLine(name = "native_log_write_timeout",
           help = "The timeout for doing log appends and truncations.")
  private static final Arg<Amount<Long, Time>> WRITE_TIMEOUT =
      Arg.create(Amount.of(3L, Time.SECONDS));

  @Retention(RUNTIME)
  @Target(PARAMETER)
  @BindingAnnotation
  private @interface LogBinding { }

  /**
   * Binds a distributed {@link com.twitter.mesos.scheduler.log.Log} that uses the mesos core native
   * log implementation.
   *
   * @param binder a guice binder to bind the distributed log with
   * @param zkKeys The keys the ZooKeeper server connection can be retrieved with.
   */
  public static void bind(Binder binder, KeyFactory zkKeys) {
    binder.install(new MesosLogStreamModule(
        Key.get(com.twitter.mesos.scheduler.log.Log.class),
        zkKeys,
        LOG_PATH.get(),
        ZK_LOG_GROUP_PATH.get()));
  }

  private final Key<com.twitter.mesos.scheduler.log.Log> logKey;
  private final File logPath;
  private final String zkPath;
  private final KeyFactory zkKeys;

  /**
   * Creates a module that binds the mesos native log to the given key.
   *
   * @param logKey The key to bind the native log implementation to.
   * @param zkKeys The keys the ZooKeeper server connection can be retrieved with.
   * @param logPath The path to the native log data directory.
   * @param zkPath The zookeeper path to use for replica coordination.
   */
  public MesosLogStreamModule(
      Key<com.twitter.mesos.scheduler.log.Log> logKey,
      KeyFactory zkKeys,
      File logPath,
      String zkPath) {

    this.logKey = Preconditions.checkNotNull(logKey);
    this.zkKeys = Preconditions.checkNotNull(zkKeys);
    this.logPath = Preconditions.checkNotNull(logPath);

    PathUtils.validatePath(zkPath);
    this.zkPath = zkPath;
  }

  @Override
  protected void configure() {
    Rebinder rebinder = Bindings.rebinder(binder(), LogBinding.class);
    rebinder.rebind(zkKeys.create(new TypeLiteral<List<InetSocketAddress>>() { }));
    rebinder.rebind(zkKeys.create(new TypeLiteral<Amount<Integer, Time>>() { }));
    rebinder.rebind(zkKeys.create(Credentials.class));

    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.ReadTimeout.class)
        .toInstance(READ_TIMEOUT.get());
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.WriteTimeout.class)
        .toInstance(WRITE_TIMEOUT.get());

    bind(logKey).to(MesosLog.class).in(Singleton.class);
    expose(logKey);
  }

  @Provides
  @Singleton
  Log provideLog(
      @LogBinding List<InetSocketAddress> endpoints,
      @LogBinding Credentials credentials,
      @LogBinding Amount<Integer, Time> sessionTimeout) {

    File parentDir = logPath.getParentFile();
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      addError("Failed to create parent directory to store native log at: %s", parentDir);
    }

    String zkConnectString =
        Joiner.on(',').join(Iterables.transform(endpoints, InetSocketAddressHelper.INET_TO_STR));

    return new Log(QUORUM_SIZE.get(),
                   logPath.getAbsolutePath(),
                   zkConnectString,
                   sessionTimeout.getValue(),
                   sessionTimeout.getUnit().getTimeUnit(),
                   zkPath,
                   credentials.scheme(),
                   credentials.authToken());
  }

  @Provides
  Log.Reader provideReader(Log log) {
    return new Log.Reader(log);
  }

  @Provides
  Log.Writer provideWriter(Log log) {
    Amount<Long, Time> electionTimeout = COORDINATOR_ELECTION_TIMEOUT.get();
    return new Log.Writer(log, electionTimeout.getValue(), electionTimeout.getUnit().getTimeUnit(),
        COORDINATOR_ELECTION_RETRIES.get());
  }

  @Provides
  @Singleton
  @MesosLog.NoopEntry
  byte[] provideNoopEntry() throws ThriftBinaryCodec.CodingException {
    return ThriftBinaryCodec.encodeNonNull(LogEntry.noop(true));
  }
}
