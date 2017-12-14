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
package org.apache.aurora.scheduler.storage.log;

import java.util.List;
import java.util.function.Consumer;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.common.application.ShutdownRegistry;
import org.apache.aurora.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import org.apache.aurora.common.collections.Pair;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.TearDownTestCase;
import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeBuildInfo;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.config.types.DataAmount;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.resources.ResourceTestUtil;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult.Quiet;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.entities.IResourceAggregate;
import org.apache.aurora.scheduler.storage.log.LogStorageModule.Options;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NonVolatileStorageTest extends TearDownTestCase {

  private FakeLog log;
  private Runnable teardown = () -> { };
  private NonVolatileStorage storage;
  private SnapshotStore snapshotStore;

  @Before
  public void setUp() {
    log = new FakeLog();
    resetStorage();
    addTearDown(teardown::run);
  }

  private void resetStorage() {
    teardown.run();

    Options options = new Options();
    options.maxLogEntrySize = new DataAmount(1, Data.GB);
    options.snapshotInterval = new TimeAmount(1, Time.DAYS);

    ShutdownRegistryImpl shutdownRegistry = new ShutdownRegistryImpl();
    Injector injector = Guice.createInjector(
        new MemStorageModule(Bindings.annotatedKeyFactory(Volatile.class)),
        new LogStorageModule(options),
        new TierModule(new TierModule.Options()),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Clock.class).toInstance(new FakeClock());
            bind(BuildInfo.class).toInstance(FakeBuildInfo.generateBuildInfo());
            bind(EventSink.class).toInstance(e -> { });
            bind(ShutdownRegistry.class).toInstance(shutdownRegistry);
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(Log.class).toInstance(log);
            bind(Snapshotter.class).to(SnapshotStoreImpl.class);
          }
        }
    );
    storage = injector.getInstance(NonVolatileStorage.class);
    snapshotStore = injector.getInstance(SnapshotStore.class);
    storage.prepare();
    storage.start(w -> { });

    teardown = () -> {
      storage.stop();
      shutdownRegistry.execute();
    };
  }

  @Test
  public void testDurability() {
    List<Pair<Quiet, Consumer<StoreProvider>>> transactions = Lists.newArrayList();

    IResourceAggregate quota = ResourceTestUtil.aggregate(2.0, 2048, 1024);
    transactions.add(Pair.of(
        stores -> {
          stores.getQuotaStore().saveQuota("lucy", quota);
        },
        stores -> {
          assertEquals(Optional.of(quota), stores.getQuotaStore().fetchQuota("lucy"));
        }
    ));
    IResourceAggregate quota2 = ResourceTestUtil.aggregate(2.0, 2048, 1024);
    transactions.add(Pair.of(
        stores -> {
          stores.getQuotaStore().saveQuota("lucy", quota2);
        },
        stores -> {
          assertEquals(Optional.of(quota2), stores.getQuotaStore().fetchQuota("lucy"));
        }
    ));
    transactions.add(Pair.of(
        stores -> {
          stores.getQuotaStore().removeQuota("lucy");
        },
        stores -> {
          assertEquals(Optional.absent(), stores.getQuotaStore().fetchQuota("lucy"));
        }
    ));

    // Walk through each transaction, simulating a storage stop/reload.
    transactions.stream()
        .forEach(transaction -> {
          storage.write(transaction.getFirst());

          resetStorage();
          storage.read(stores -> {
            transaction.getSecond().accept(stores);
            return null;
          });

          // Result should survive another reset.
          snapshotStore.snapshot();
          resetStorage();
          storage.read(stores -> {
            transaction.getSecond().accept(stores);
            return null;
          });
        });
  }
}
