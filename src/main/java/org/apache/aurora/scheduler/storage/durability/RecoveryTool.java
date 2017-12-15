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
package org.apache.aurora.scheduler.storage.durability;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Map;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.IStringConverterFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.app.LifecycleModule;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.config.converters.DataAmountConverter;
import org.apache.aurora.scheduler.config.converters.InetSocketAddressConverter;
import org.apache.aurora.scheduler.config.converters.TimeAmountConverter;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.discovery.FlaggedZooKeeperConfig;
import org.apache.aurora.scheduler.discovery.ServiceDiscoveryBindings;
import org.apache.aurora.scheduler.log.mesos.MesosLogStreamModule;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.backup.BackupReader;
import org.apache.aurora.scheduler.storage.log.LogPersistenceModule;
import org.apache.aurora.scheduler.storage.log.SnapshotterImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to recover the contents of one persistence into another.
 */
public final class RecoveryTool {

  private static final Logger LOG = LoggerFactory.getLogger(RecoveryTool.class);

  private RecoveryTool() {
    // Main-only class.
  }

  interface RecoveryEndpoint {
    Iterable<Object> getOptions();

    Persistence create();
  }

  private static class Log implements RecoveryEndpoint {
    private final FlaggedZooKeeperConfig.Options zkOptions = new FlaggedZooKeeperConfig.Options();
    private final MesosLogStreamModule.Options logOptions = new MesosLogStreamModule.Options();
    private final LogPersistenceModule.Options options = new LogPersistenceModule.Options();

    @Override
    public Iterable<Object> getOptions() {
      return ImmutableList.of(logOptions, options, zkOptions);
    }

    @Override
    public Persistence create() {
      Injector injector = Guice.createInjector(
          new TierModule(TaskTestUtil.TIER_CONFIG),
          new MesosLogStreamModule(logOptions, FlaggedZooKeeperConfig.create(zkOptions)),
          new LogPersistenceModule(options),
          new LifecycleModule(),
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(ServiceDiscoveryBindings.ZOO_KEEPER_CLUSTER_KEY)
                  .toInstance(zkOptions.zkEndpoints);
              bind(Snapshotter.class).to(SnapshotterImpl.class);
              bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
              bind(BuildInfo.class).toInstance(new BuildInfo());
            }
          });
      return injector.getInstance(Persistence.class);
    }
  }

  private static class Backup implements RecoveryEndpoint {
    @Parameters(separators = "=")
    private static class Options {
      @Parameter(names = "-backup", description = "Backup file to load")
      File backup;
    }

    private final Options options = new Options();

    @Override
    public Iterable<Object> getOptions() {
      return ImmutableList.of(options);
    }

    @Override
    public Persistence create() {
      return new BackupReader(
          options.backup,
          new SnapshotterImpl(new BuildInfo(), Clock.SYSTEM_CLOCK));
    }
  }

  enum Endpoint {
    LOG(new Log()),
    BACKUP(new Backup());

    private final RecoveryEndpoint impl;

    Endpoint(RecoveryEndpoint impl) {
      this.impl = impl;
    }
  }

  @Parameters(separators = "=")
  private static class Options {
    @Parameter(names = "-from",
        required = true,
        description = "Persistence to read state from")
    Endpoint from;

    @Parameter(names = "-to",
        required = true,
        description = "Persistence to write recovered state into")
    Endpoint to;

    @Parameter(names = "-batch-size",
        description = "Write in batches of this may ops.")
    int batchSize = 50;

    @Parameter(names = "--help", description = "Print usage", help = true)
    boolean help;
  }

  private static JCommander configure(Options options, String... args) {
    JCommander.Builder builder = JCommander.newBuilder().programName(RecoveryTool.class.getName());
    builder.addConverterFactory(new IStringConverterFactory() {
      private Map<Class<?>, Class<? extends IStringConverter<?>>> classConverters =
          ImmutableMap.<Class<?>, Class<? extends IStringConverter<?>>>builder()
              .put(DataAmount.class, DataAmountConverter.class)
              .put(InetSocketAddress.class, InetSocketAddressConverter.class)
              .put(TimeAmount.class, TimeAmountConverter.class)
              .build();

      @SuppressWarnings("unchecked")
      @Override
      public <T> Class<? extends IStringConverter<T>> getConverter(Class<T> forType) {
        return (Class<IStringConverter<T>>) classConverters.get(forType);
      }
    });

    builder.addObject(options);
    for (Endpoint endpoint : Endpoint.values()) {
      endpoint.impl.getOptions().forEach(builder::addObject);
    }

    JCommander parser = builder.build();
    parser.parse(args);
    return parser;
  }

  public static void main(String[] args) {
    Options options = new Options();
    JCommander parser = configure(options, args);
    if (options.help) {
      parser.usage();
      System.exit(1);
    }

    LOG.info("Recovering from " + options.from + " to " + options.to);
    Persistence from = options.from.impl.create();
    Persistence to = options.to.impl.create();

    from.prepare();
    to.prepare();

    Recovery.copy(from, to, options.batchSize);
  }
}
