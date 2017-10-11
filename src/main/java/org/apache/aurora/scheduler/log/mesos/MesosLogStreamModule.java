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
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Singleton;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.common.net.InetSocketAddressHelper;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.discovery.ServiceDiscoveryBindings;
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
  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-native_log_quorum_size",
        description = "The size of the quorum required for all log mutations.")
    public int quorumSize = 1;

    @Parameter(names = "-native_log_file_path",
        description =
            "Path to a file to store the native log data in.  If the parent directory does"
                + "not exist it will be created.")
    public File logPath = null;

    @Parameter(names = "-native_log_zk_group_path",
        description = "A zookeeper node for use by the native log to track the master coordinator.")
    public String zkLogGroupPath = null;

    /*
     * This timeout includes the time to get a quorum to promise leadership to the coordinator and
     * the time to fill any holes in the coordinator's log.
     */
    @Parameter(names = "-native_log_election_timeout",
        description = "The timeout for a single attempt to obtain a new log writer.")
    public TimeAmount coordinatorElectionTimeout = new TimeAmount(15, Time.SECONDS);

    /**
     * Normally retries would not be expected to help much - however in the small replica set where
     * a few down replicas doom a coordinator election attempt, retrying effectively gives us a
     * wider window in which to await a live quorum before giving up and thrashing the global
     * election process.  Observed log replica recovery times as of 4/6/2012 can be ~45 seconds so
     * giving a window >= 2x this should support 1-round election events (that possibly use several
     * retries in the single round).
     */
    @Parameter(names = "-native_log_election_retries",
        description = "The maximum number of attempts to obtain a new log writer.")
    public int coordinatorElectionRetries = 20;

    @Parameter(names = "-native_log_read_timeout",
        description = "The timeout for doing log reads.")
    public TimeAmount readTimeout = new TimeAmount(5, Time.SECONDS);

    @Parameter(names = "-native_log_write_timeout",
        description = "The timeout for doing log appends and truncations.")
    public TimeAmount writeTimeout = new TimeAmount(3, Time.SECONDS);
  }

  private static void requireArg(Object arg, String name) {
    if (arg == null) {
      throw new IllegalArgumentException(
          String.format("A value for the -%s flag must be supplied", name));
    }
  }

  private final Options options;
  private final ZooKeeperConfig zkClientConfig;

  public MesosLogStreamModule(Options options, ZooKeeperConfig zkClientConfig) {
    this.options = options;
    requireArg(options.logPath, "native_log_file_path");
    requireArg(options.zkLogGroupPath, "native_log_zk_group_path");
    PathUtils.validatePath(options.zkLogGroupPath);
    this.zkClientConfig = zkClientConfig;
  }

  @Override
  protected void configure() {
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.ReadTimeout.class)
        .toInstance(options.readTimeout);
    bind(new TypeLiteral<Amount<Long, Time>>() { }).annotatedWith(MesosLog.WriteTimeout.class)
        .toInstance(options.writeTimeout);

    bind(org.apache.aurora.scheduler.log.Log.class).to(MesosLog.class);
    bind(MesosLog.class).in(Singleton.class);
    expose(org.apache.aurora.scheduler.log.Log.class);
  }

  @Provides
  @Singleton
  Log provideLog(@ServiceDiscoveryBindings.ZooKeeper Iterable<InetSocketAddress> servers) {
    File parentDir = options.logPath.getParentFile();
    if (!parentDir.exists() && !parentDir.mkdirs()) {
      addError("Failed to create parent directory to store native log at: %s", parentDir);
    }

    String zkConnectString = Joiner.on(',').join(
        Iterables.transform(servers, InetSocketAddressHelper::toString));

    if (zkClientConfig.getCredentials().isPresent()) {
      Credentials zkCredentials = zkClientConfig.getCredentials().get();
      return new Log(
          options.quorumSize,
          options.logPath.getAbsolutePath(),
          zkConnectString,
          zkClientConfig.getSessionTimeout().getValue(),
          zkClientConfig.getSessionTimeout().getUnit().getTimeUnit(),
          options.zkLogGroupPath,
          zkCredentials.scheme(),
          zkCredentials.authToken());
    } else {
      return new Log(
          options.quorumSize,
          options.logPath.getAbsolutePath(),
          zkConnectString,
          zkClientConfig.getSessionTimeout().getValue(),
          zkClientConfig.getSessionTimeout().getUnit().getTimeUnit(),
          options.zkLogGroupPath);
    }
  }

  @Provides
  Log.Reader provideReader(Log log) {
    return new Log.Reader(log);
  }

  @Provides
  Log.Writer provideWriter(Log log) {
    Amount<Long, Time> electionTimeout = options.coordinatorElectionTimeout;
    return new Log.Writer(
        log,
        electionTimeout.getValue(),
        electionTimeout.getUnit().getTimeUnit(),
        options.coordinatorElectionRetries);
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
