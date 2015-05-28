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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

public class UpdateStoreBenchmarks {

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class JobDetailsBenchmark {
    private static final String USER = "user";
    private Storage storage;
    private Set<IJobUpdateKey> keys;

    @Param({"1000", "5000", "10000"})
    private int instances;

    @Setup(Level.Trial)
    public void setUp() {
      storage = DbUtil.createStorage();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        protected void execute(Storage.MutableStoreProvider storeProvider) {
          JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
          Set<IJobUpdateDetails> updates =
              new JobUpdates.Builder().setNumInstanceEvents(instances).build(1);

          ImmutableSet.Builder<IJobUpdateKey> keyBuilder = ImmutableSet.builder();
          for (IJobUpdateDetails details : updates) {
            IJobUpdateKey key = details.getUpdate().getSummary().getKey();
            keyBuilder.add(key);
            String lockToken = UUID.randomUUID().toString();
            storeProvider.getLockStore().saveLock(
                ILock.build(new Lock(LockKey.job(key.getJob().newBuilder()), lockToken, USER, 0L)));

            updateStore.saveJobUpdate(details.getUpdate(), Optional.of(lockToken));

            for (IJobUpdateEvent updateEvent : details.getUpdateEvents()) {
              updateStore.saveJobUpdateEvent(key, updateEvent);
            }

            for (IJobInstanceUpdateEvent instanceEvent : details.getInstanceEvents()) {
              updateStore.saveJobInstanceUpdateEvent(key, instanceEvent);
            }
          }
          keys = keyBuilder.build();
        }
      });
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        protected void execute(Storage.MutableStoreProvider storeProvider) {
          storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
          storeProvider.getLockStore().deleteLocks();
        }
      });
    }

    @Benchmark
    public IJobUpdateDetails run() throws TException {
      return storage.read(new Storage.Work.Quiet<IJobUpdateDetails>() {
        @Override
        public IJobUpdateDetails apply(Storage.StoreProvider store) {
          return store.getJobUpdateStore().fetchJobUpdateDetails(
              Iterables.getOnlyElement(keys)).get();
        }
      });
    }
  }
}
