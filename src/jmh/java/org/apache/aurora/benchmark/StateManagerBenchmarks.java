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
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.benchmark.fakes.FakeDriver;
import org.apache.aurora.benchmark.fakes.FakeEventSink;
import org.apache.aurora.benchmark.fakes.FakeRescheduleCalculator;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.async.AsyncModule;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.state.StateManager;
import org.apache.aurora.scheduler.state.StateManagerImpl;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.DbModule;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;
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

public class StateManagerBenchmarks {

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 10, time = 30, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class InsertPendingTasksBenchmark {
    private StateManager manager;
    private Storage storage;

    @Param({"1000", "10000", "50000"})
    private int numPendingTasks;

    // Used to prevent job key collisions
    private int numIterations = 0;

    @Setup
    public void setUp() {
      Injector injector = getInjector();
      manager = injector.getInstance(StateManager.class);
      storage = injector.getInstance(Storage.class);
      storage.prepare();
    }

    @TearDown
    public void tearDown() {
      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        public void execute(Storage.MutableStoreProvider storeProvider) throws RuntimeException {
          storeProvider.getUnsafeTaskStore().deleteAllTasks();
        }
      });
    }

    @Benchmark
    public Set<Integer> run() {
      IScheduledTask task =
          Iterables.getOnlyElement(new Tasks.Builder().setJob("iter_" + numIterations).build(1));
      ITaskConfig config = task.getAssignedTask().getTask();
      Set<Integer> taskIds =
          IntStream.range(0, numPendingTasks).boxed().collect(Collectors.toSet());

      numIterations++;

      return storage.write((Storage.MutateWork.Quiet<Set<Integer>>) storeProvider -> {
        manager.insertPendingTasks(
            storeProvider,
            config,
            taskIds
        );

        return taskIds;
      });
    }
  }

  @BenchmarkMode(Mode.Throughput)
  @OutputTimeUnit(TimeUnit.SECONDS)
  @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
  @Measurement(iterations = 10, time = 30, timeUnit = TimeUnit.SECONDS)
  @Fork(1)
  @State(Scope.Thread)
  public static class DeleteTasksBenchmark {
    private StateManager manager;
    private Storage storage;
    private Set<String> taskIds;

    @Param({"1000", "10000", "50000"})
    private int numTasksToDelete;

    @Setup(Level.Trial)
    public void setUpStorage() {
      Injector injector = getInjector();
      manager = injector.getInstance(StateManager.class);
      storage = injector.getInstance(Storage.class);
      storage.prepare();
    }

    // JMH warns heavily against using `Invocation` but this test seems to meet the requirements
    // of using it. Each benchmark will take more than one ms and it avoids awkward logic to
    // setup storage before the benchmark.
    @Setup(Level.Invocation)
    public void setUp() {
      storage.write(new Storage.MutateWork.NoResult.Quiet() {
        @Override
        public void execute(Storage.MutableStoreProvider storeProvider) throws RuntimeException {
          taskIds = bulkInsertTasks(numTasksToDelete, storeProvider.getUnsafeTaskStore());
        }
      });
    }

    @Benchmark
    public Set<String> run() {
      return storage.write((Storage.MutateWork.Quiet<Set<String>>) storeProvider -> {
        manager.deleteTasks(storeProvider, taskIds);
        return taskIds;
      });
    }
  }

  private static Set<String> bulkInsertTasks(int num, TaskStore.Mutable store) {
    Set<IScheduledTask> tasks =
        new Tasks.Builder().setScheduleStatus(ScheduleStatus.FINISHED).build(num);
    store.saveTasks(tasks);

    return tasks.stream().map(t -> t.getAssignedTask().getTaskId()).collect(Collectors.toSet());
  }

  private static Injector getInjector() {
    return Guice.createInjector(
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Clock.class).toInstance(Clock.SYSTEM_CLOCK);
            bind(Driver.class).toInstance(new FakeDriver());
            bind(EventSink.class).toInstance(new FakeEventSink());
            // We want to measure the throughput of the state manager so we fake out the
            // rescheduling calculator.
            bind(RescheduleCalculator.class).toInstance(new FakeRescheduleCalculator());
            bind(TaskIdGenerator.class).to(TaskIdGenerator.TaskIdGeneratorImpl.class);
            // This is what we want to benchmark
            bind(StateManager.class).to(StateManagerImpl.class);
            // This is needed for storage
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
          }
        },
        DbModule.productionModule(Bindings.KeyFactory.PLAIN),
        // This is needed for storage
        new AsyncModule()
    );
  }
}
