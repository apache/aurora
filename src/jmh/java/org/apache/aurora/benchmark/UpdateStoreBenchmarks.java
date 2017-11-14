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
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Iterables;

import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
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
    private Storage storage;
    private Set<IJobUpdateKey> keys;

    @Param({"1000", "5000", "10000"})
    private int instances;

    @Setup(Level.Trial)
    public void setUp() {
      storage = MemStorageModule.newEmptyStorage();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      keys = JobUpdates.saveUpdates(
          storage,
          new JobUpdates.Builder().setNumInstanceEvents(instances).build(1));
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      storage.write((NoResult.Quiet) storeProvider -> {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
      });
    }

    @Benchmark
    public IJobUpdateDetails run() throws TException {
      return storage.read(store -> store.getJobUpdateStore().fetchJobUpdateDetails(
          Iterables.getOnlyElement(keys)).get());
    }
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class JobInstructionsBenchmark {
    private Storage storage;
    private Set<IJobUpdateKey> keys;

    @Param({"1", "10", "100", "1000"})
    private int instanceOverrides;

    @Setup(Level.Trial)
    public void setUp() {
      storage = MemStorageModule.newEmptyStorage();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      keys = JobUpdates.saveUpdates(
          storage,
          new JobUpdates.Builder().setNumInstanceOverrides(instanceOverrides).build(1));
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      storage.write((NoResult.Quiet) storeProvider -> {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
      });
    }

    @Benchmark
    public IJobUpdateDetails run() throws TException {
      return storage.read(store -> store.getJobUpdateStore().fetchJobUpdateDetails(
          Iterables.getOnlyElement(keys)).get());
    }
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 5, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class JobUpdateMetadataBenchmark {
    private Storage storage;
    private Set<IJobUpdateKey> keys;

    @Param({"10", "100", "1000", "10000"})
    private int metadata;

    @Setup(Level.Trial)
    public void setUp() {
      storage = MemStorageModule.newEmptyStorage();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      keys = JobUpdates.saveUpdates(
          storage,
          new JobUpdates.Builder().setNumUpdateMetadata(metadata).build(1));
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      storage.write((NoResult.Quiet) storeProvider -> {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
      });
    }

    @Benchmark
    public IJobUpdateDetails run() throws TException {
      return storage.read(store -> store.getJobUpdateStore().fetchJobUpdateDetails(
          Iterables.getOnlyElement(keys)).get());
    }
  }
}
