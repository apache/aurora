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

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.inject.Bindings;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.apache.aurora.scheduler.storage.mem.MemStorage;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
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
    private IJobUpdateKey key;

    @Param({"1000", "5000", "10000"})
    private int instances;

    @Setup(Level.Trial)
    public void setUp() {
      Injector injector = Guice.createInjector(
          new AbstractModule() {
            @Override
            protected void configure() {
              bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
            }
          },
          new MemStorageModule(Bindings.KeyFactory.PLAIN),
          new DbModule(Bindings.annotatedKeyFactory(MemStorage.Delegated.class)));
      storage = injector.getInstance(Storage.class);
      storage.prepare();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      final JobKey job = new JobKey("role", "env", UUID.randomUUID().toString());
      key = IJobUpdateKey.build(new JobUpdateKey()
          .setJob(job)
          .setId(UUID.randomUUID().toString()));

      TaskConfig task = new TaskConfig()
          .setJob(job)
          .setExecutorConfig(new ExecutorConfig("cfg", string(10000)));

      final JobUpdate update = new JobUpdate()
          .setSummary(new JobUpdateSummary()
              .setKey(key.newBuilder())
              .setUpdateId(key.getId())
              .setJobKey(job)
              .setUser(USER))
          .setInstructions(new JobUpdateInstructions()
              .setSettings(new JobUpdateSettings()
                  .setUpdateGroupSize(100)
                  .setMaxFailedInstances(1)
                  .setMaxPerInstanceFailures(1)
                  .setMaxWaitToInstanceRunningMs(1)
                  .setMinWaitInInstanceRunningMs(1)
                  .setRollbackOnFailure(true)
                  .setWaitForBatchCompletion(false))
              .setInitialState(ImmutableSet.of(new InstanceTaskConfig()
                  .setTask(task)
                  .setInstances(ImmutableSet.of(new Range(0, 10)))))
              .setDesiredState(new InstanceTaskConfig()
                  .setTask(task)
                  .setInstances(ImmutableSet.of(new Range(0, instances)))));

      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        protected void execute(Storage.MutableStoreProvider storeProvider) {
          String lockToken = UUID.randomUUID().toString();
          storeProvider.getLockStore().saveLock(
              ILock.build(new Lock(LockKey.job(job), lockToken, USER, 0L)));

          JobUpdateStore.Mutable updateStore = storeProvider.getJobUpdateStore();
          updateStore.saveJobUpdate(IJobUpdate.build(update), Optional.of(lockToken));
          updateStore.saveJobUpdateEvent(
              key,
              IJobUpdateEvent.build(new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 0L)
                  .setUser(USER)
                  .setMessage("message")));

          for (int i = 0; i < instances; i++) {
            updateStore.saveJobInstanceUpdateEvent(
                key,
                IJobInstanceUpdateEvent.build(
                    new JobInstanceUpdateEvent(i, 0L, JobUpdateAction.INSTANCE_UPDATING)));
          }
        }
      });
    }

    private static String string(int numChars) {
      char[] chars = new char[numChars];
      Arrays.fill(chars, 'a');
      return new String(chars);
    }

    @Benchmark
    public IJobUpdateDetails run() throws TException {
      return storage.read(new Storage.Work.Quiet<IJobUpdateDetails>() {
        @Override
        public IJobUpdateDetails apply(Storage.StoreProvider store) {
          return store.getJobUpdateStore().fetchJobUpdateDetails(key).get();
        }
      });
    }
  }
}
