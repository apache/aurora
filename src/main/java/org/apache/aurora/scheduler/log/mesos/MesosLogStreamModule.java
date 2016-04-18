/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.aurora.scheduler.log.mesos;

import java.io.File;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Singleton;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.common.net.InetSocketAddressHelper;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.scheduler.discovery.ZooKeeperConfig;
import org.apache.aurora.scheduler.log.mesos.LogInterface.ReaderInterface;
import org.apache.aurora.scheduler.log.mesos.LogInterface.WriterInterface;
import org.apache.mesos.Log;
import org.apache.zookeeper.common.PathUtils;

/**
 * Binds a native mesos Log implementation.
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

  private static <T> T getRequiredArg(Arg<T> arg, String name) {
    if (!arg.hasAppliedValue()) {
      throw new IllegalStateException(
          String.format("A value for the -%s flag must be supplied", name));
    }
    return arg.get();
  }

  private final ZooKeeperConfig zkClientConfig;
  private final File logPath;
  private final String zkLogGroupPath;

  public MesosLogStreamModule(ZooKeeperConfig zkClientConfig) {
    this(zkClientConfig,
        getRequiredArg(LOG_PATH, "native_log_file_path"),
        getRequiredArg(ZK_LOG_GROUP_PATH, "native_log_zk_group_path"));
  }

  public MesosLogStreamModule(
      ZooKeeperConfig zkClientConfig,
      File logPath,
      String zkLogGroupPath) {

    this.zkClientConfig = Objects.requireNonNull(zkClientConfig);
    this.logPath = Objects.requireNonNull(logPath);

    PathUtils.validatePath(zkLogGroupPath); // This checks for null.
    this.zkLogGroupPath = zkLogGroupPath;
  }

  @Override
  protected void configure() {
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.ReadTimeout.class)
        .toInstance(READ_TIMEOUT.get());
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.WriteTimeout.class)
        .toInstance(WRITE_TIMEOUT.get());

    bind(org.apache.aurora.scheduler.log.Log.class).to(MesosLog.class);
    bind(MesosLog.class).in(Singleton.class);
    expose(org.apache.aurora.scheduler.log.Log.class);
  }

  @Provides
  @Singleton
  Log provideLog() {
    File parentDir = logPath.getParentFile();
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      addError("Failed to create parent directory to store native log at: %s", parentDir);
    }

    String zkConnectString = Joiner.on(',').join(
        Iterables.transform(zkClientConfig.getServers(), InetSocketAddressHelper::toString));

    if (zkClientConfig.getCredentials().isPresent()) {
      Credentials zkCredentials = zkClientConfig.getCredentials().get();
      return new Log(
          QUORUM_SIZE.get(),
          logPath.getAbsolutePath(),
          zkConnectString,
          zkClientConfig.getSessionTimeout().getValue(),
          zkClientConfig.getSessionTimeout().getUnit().getTimeUnit(),
          zkLogGroupPath,
          zkCredentials.scheme(),
          zkCredentials.authToken());
    } else {
      return new Log(
          QUORUM_SIZE.get(),
          logPath.getAbsolutePath(),
          zkConnectString,
          zkClientConfig.getSessionTimeout().getValue(),
          zkClientConfig.getSessionTimeout().getUnit().getTimeUnit(),
          zkLogGroupPath);
    }
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
  LogInterface provideLogInterface(final Log log) {
    return log::position;
  }

  @Provides
  ReaderInterface provideReaderInterface(final Log.Reader reader) {
    return new ReaderInterface() {
      @Override
      public List<Log.Entry> read(Log.Position from, Log.Position to, long timeout, TimeUnit unit)
          throws TimeoutException, Log.OperationFailedException {

        return reader.read(from, to, timeout, unit);
      }

      @Override
      public Log.Position beginning() {
        return reader.beginning();
      }

      @Override
      public Log.Position ending() {
        return reader.ending();
      }
    };
  }

  @Provides
  WriterInterface provideWriterInterface(final Log.Writer writer) {
    return new WriterInterface() {
      @Override
      public Log.Position append(byte[] data, long timeout, TimeUnit unit)
          throws TimeoutException, Log.WriterFailedException {
        return writer.append(data, timeout, unit);
      }

      @Override
      public Log.Position truncate(Log.Position to, long timeout, TimeUnit unit)
          throws TimeoutException, Log.WriterFailedException {
        return writer.truncate(to, timeout, unit);
      }
    };
  }

  @Provides
  @Singleton
  @MesosLog.NoopEntry
  byte[] provideNoopEntry() throws ThriftBinaryCodec.CodingException {
    return ThriftBinaryCodec.encodeNonNull(LogEntry.noop(true));
  }
}
