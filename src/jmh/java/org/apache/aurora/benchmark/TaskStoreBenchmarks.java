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

import com.google.common.base.Optional;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.util.Modules;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.mem.InMemStoresModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
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

import static org.apache.aurora.common.inject.Bindings.KeyFactory.PLAIN;

public class TaskStoreBenchmarks {

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public abstract static class AbstractFetchTasksBenchmark {
    protected Storage storage;
    public abstract void setUp();

    @Param({"10000", "50000", "100000"})
    protected int numTasks;

    protected void createTasks(int size) {
      storage.write((Storage.MutateWork.NoResult.Quiet) storeProvider -> {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        Set<IScheduledTask> tasks = new Tasks.Builder().build(size);
        taskStore.saveTasks(tasks);
      });
    }

    protected void deleteTasks() {
      storage.write((Storage.MutateWork.NoResult.Quiet) storeProvider -> {
        TaskStore.Mutable taskStore = storeProvider.getUnsafeTaskStore();
        taskStore.deleteAllTasks();
      });
    }
  }

  public static class MemFetchTasksBenchmark extends AbstractFetchTasksBenchmark {
    @Setup(Level.Trial)
    @Override
    public void setUp() {
      storage = Guice.createInjector(
          Modules.combine(
              DbModule.testModuleWithWorkQueue(PLAIN, Optional.of(new InMemStoresModule(PLAIN))),
              new AbstractModule() {
                @Override
                protected void configure() {
                  bind(StatsProvider.class).toInstance(new FakeStatsProvider());
                }
              }))
          .getInstance(Storage.class);

    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      createTasks(numTasks);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      deleteTasks();
    }

    @Benchmark
    public Iterable<IScheduledTask> run() {
      return storage.read(store -> store.getTaskStore().fetchTasks(Query.unscoped()));
    }
  }

  public static class DBFetchTasksBenchmark extends AbstractFetchTasksBenchmark {
    @Setup(Level.Trial)
    @Override
    public void setUp() {
      storage = DbUtil.createStorage();
    }

    @Setup(Level.Iteration)
    public void setUpIteration() {
      createTasks(numTasks);
    }

    @TearDown(Level.Iteration)
    public void tearDownIteration() {
      deleteTasks();
    }

    @Benchmark
    public Iterable<IScheduledTask> run() {
      return storage.read(store -> store.getTaskStore().fetchTasks(Query.unscoped()));
    }
  }
}
