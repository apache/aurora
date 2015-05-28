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
package org.apache.aurora.benchmark;

import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.twitter.common.inject.Bindings;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Clock;

import org.apache.aurora.benchmark.fakes.FakeStatsProvider;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.thrift.TException;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

/**
 * Performance benchmarks for snapshot related operations.
 */
public class SnapshotBenchmarks {
  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @Threads(1)
  @State(Scope.Thread)
  public static class RestoreSnapshotWithUpdatesBenchmark {
    private SnapshotStoreImpl snapshotStore;
    private Snapshot snapshot;

    @Param({"1", "5", "10"})
    private int updateCount;

    @Setup(Level.Trial)
    public void setUp() {
      snapshotStore = getSnapshotStore();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      snapshot = createSnapshot(updateCount, 100, 10000);
    }

    @Benchmark
    public boolean run() throws TException {
      snapshotStore.applySnapshot(snapshot);
      // Return non-guessable result to satisfy "blackhole" requirement.
      return System.currentTimeMillis() % 5 == 0;
    }
  }

  private static SnapshotStoreImpl getSnapshotStore() {
    Bindings.KeyFactory keyFactory = Bindings.annotatedKeyFactory(Storage.Volatile.class);
    Injector injector = Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(SnapshotStoreImpl.class).in(Singleton.class);
          }
        },
        DbModule.testModule(keyFactory, Optional.of(new DbModule.TaskStoreModule(keyFactory))));

    Storage storage = injector.getInstance(Key.get(Storage.class, Storage.Volatile.class));
    storage.prepare();
    return injector.getInstance(SnapshotStoreImpl.class);
  }

  private static Snapshot createSnapshot(int updates, int events, int instanceEvents) {
    Set<IJobUpdateDetails> updateDetails = new JobUpdates.Builder()
        .setNumEvents(events)
        .setNumInstanceEvents(instanceEvents)
        .build(updates);

    ImmutableSet.Builder<Lock> lockBuilder = ImmutableSet.builder();
    ImmutableSet.Builder<StoredJobUpdateDetails> detailsBuilder = ImmutableSet.builder();
    for (IJobUpdateDetails details : updateDetails) {
      IJobUpdateKey key = details.getUpdate().getSummary().getKey();
      String lockToken = UUID.randomUUID().toString();

      lockBuilder.add(new Lock(LockKey.job(key.getJob().newBuilder()), lockToken, "user", 0L));
      detailsBuilder.add(new StoredJobUpdateDetails(details.newBuilder(), lockToken));
    }

    return new Snapshot()
        .setLocks(lockBuilder.build())
        .setJobUpdateDetails(detailsBuilder.build());
  }
}
