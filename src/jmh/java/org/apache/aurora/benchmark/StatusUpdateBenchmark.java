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

import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Provides;

import com.twitter.common.application.ShutdownStage;
import com.twitter.common.base.Command;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.StatsProvider;
import com.twitter.common.util.Clock;
import com.twitter.common.util.testing.FakeClock;

import org.apache.aurora.benchmark.fakes.FakeOfferManager;
import org.apache.aurora.benchmark.fakes.FakeRescheduleCalculator;
import org.apache.aurora.benchmark.fakes.FakeSchedulerDriver;
import org.apache.aurora.benchmark.fakes.FakeStatsProvider;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.TaskLauncher;
import org.apache.aurora.scheduler.UserTaskLauncher;
import org.apache.aurora.scheduler.async.OfferManager;
import org.apache.aurora.scheduler.async.RescheduleCalculator;
import org.apache.aurora.scheduler.async.preemptor.ClusterStateImpl;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.mesos.DriverFactory;
import org.apache.aurora.scheduler.mesos.DriverSettings;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.aurora.scheduler.mesos.SchedulerDriverModule;
import org.apache.aurora.scheduler.state.StateModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
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

import static java.util.Objects.requireNonNull;

/**
 * Performance benchmarks for status update processing throughput. Note that we
 * need to send many updates and wait for all transitions to occur within one run
 * of the benchmark. This is because we don't want to assume that status updates
 * are processed synchronously.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
public class StatusUpdateBenchmark {
  /**
   * Simulates a slow storage backend by introducing latency on top of an
   * underlying storage implementation.
   *
   * TODO(bmahler): Consider specifying read and write latency separately.
   */
  private static final class SlowStorageWrapper implements Storage {
    private final Storage underlyingStorage;
    private Optional<Amount<Long, Time>> latency = Optional.absent();

    private SlowStorageWrapper(Storage underlyingStorage) {
      this.underlyingStorage = requireNonNull(underlyingStorage);
    }

    private void setLatency(Amount<Long, Time> latency) {
      this.latency = Optional.of(latency);
    }

    private void maybeSleep() {
      if (latency.isPresent()) {
        Uninterruptibles.sleepUninterruptibly(
            latency.get().getValue(),
            latency.get().getUnit().getTimeUnit());
      }
    }

    @Override
    public <T, E extends Exception> T read(Work<T, E> work) throws StorageException, E {
      maybeSleep();
      return underlyingStorage.read(work);
    }

    @Override
    public <T, E extends Exception> T write(MutateWork<T, E> work) throws StorageException, E {
      maybeSleep();
      return underlyingStorage.write(work);
    }

    @Override
    public <E extends Exception> void bulkLoad(MutateWork.NoResult<E> work)
        throws StorageException, E {

      maybeSleep();
      underlyingStorage.bulkLoad(work);
    }

    @Override
    public void prepare() throws StorageException {
      underlyingStorage.prepare();
    }
  }

  // Benchmark with 1000 tasks to easily observe the kilo-qps of status
  // update processing. Consider varying this number if needed.
  private static final int NUM_TASKS = 1000;

  // Vary the storage latency to observe the effect on throughput.
  @Param({"5", "25", "100"})
  private long latencyMilliseconds;

  private SchedulerDriver driver;
  private Scheduler scheduler;
  private SlowStorageWrapper storage;
  private EventBus eventBus;
  private Set<IScheduledTask> tasks;
  private CountDownLatch countDownLatch;

  /**
   * Run once per trial to set up the benchmark.
   */
  @Setup(Level.Trial)
  public void setUpBenchmark() {
    eventBus = new EventBus();
    storage = new SlowStorageWrapper(DbUtil.createStorage());

    Injector injector = Guice.createInjector(
        new StateModule(),
        new SchedulerDriverModule(),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(DriverFactory.class).toInstance(new DriverFactory() {
              @Override
              public SchedulerDriver create(
                  Scheduler s,
                  Optional<Protos.Credential> credentials,
                  Protos.FrameworkInfo frameworkInfo,
                  String master) {

                return new FakeSchedulerDriver();
              }
            });
            bind(OfferManager.class).toInstance(new FakeOfferManager());
            bind(TaskIdGenerator.class).to(TaskIdGenerator.TaskIdGeneratorImpl.class);
            bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);
            bind(Command.class).annotatedWith(ShutdownStage.class).toInstance(
                new Command() {
                  @Override
                  public void execute() throws RuntimeException {
                    // no-op
                  }
                });
            bind(Thread.UncaughtExceptionHandler.class).toInstance(
                new Thread.UncaughtExceptionHandler() {
                  @Override
                  public void uncaughtException(Thread t, Throwable e) {
                    // no-op
                  }
                });
            bind(Storage.class).toInstance(storage);
            bind(DriverSettings.class).toInstance(
                new DriverSettings(
                    "fakemaster",
                    Optional.<Protos.Credential>absent(),
                    Protos.FrameworkInfo.newBuilder()
                        .setUser("framework user")
                        .setName("test framework")
                        .build()));
            bind(RescheduleCalculator.class).toInstance(new FakeRescheduleCalculator());
            bind(Clock.class).toInstance(new FakeClock());
            bind(ExecutorSettings.class)
                .toInstance(ExecutorSettings.newBuilder()
                    .setExecutorPath("/executor/thermos")
                    .setThermosObserverRoot("/var/run/thermos")
                    .build());
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(EventSink.class).toInstance(new EventSink() {
              @Override
              public void post(PubsubEvent event) {
                eventBus.post(event);
              }
            });
          }

          @Provides
          @Singleton
          List<TaskLauncher> provideTaskLaunchers(
              UserTaskLauncher userTaskLauncher) {
            return ImmutableList.<TaskLauncher>of(userTaskLauncher);
          }
        }
    );

    eventBus.register(injector.getInstance(ClusterStateImpl.class));
    scheduler = injector.getInstance(Scheduler.class);
    eventBus.register(this);
  }

  /**
   * Runs before each iteration of the benchmark in order to vary the storage
   * latency across iterations, based on the latency parameter.
   */
  @Setup(Level.Iteration)
  public void setIterationLatency() {
    storage.setLatency(Amount.of(latencyMilliseconds, Time.MILLISECONDS));
  }

  /**
   * Runs before each invocation of the benchmark in order to store the tasks
   * that we will transition in the benchmark.
   */
  @Setup(Level.Invocation)
  public void createTasks() {
    tasks = new Tasks.Builder()
        .setScheduleStatus(ScheduleStatus.STARTING)
        .build(NUM_TASKS);

    storage.write(new Storage.MutateWork.NoResult.Quiet() {
      @Override
      protected void execute(Storage.MutableStoreProvider storeProvider) {
        storeProvider.getUnsafeTaskStore().saveTasks(tasks);
      }
    });

    countDownLatch = new CountDownLatch(tasks.size());
  }

  @Subscribe
  public void taskChangedState(PubsubEvent.TaskStateChange stateChange) {
    countDownLatch.countDown();
  }

  @Benchmark
  public boolean runBenchmark() throws InterruptedException {
    for (String taskId : org.apache.aurora.scheduler.base.Tasks.ids(tasks)) {
      Protos.TaskStatus status = Protos.TaskStatus.newBuilder()
          .setState(Protos.TaskState.TASK_RUNNING)
          .setSource(Protos.TaskStatus.Source.SOURCE_EXECUTOR)
          .setMessage("message")
          .setTimestamp(1D)
          .setTaskId(Protos.TaskID.newBuilder().setValue(taskId).build())
          .build();

      scheduler.statusUpdate(new FakeSchedulerDriver(), status);
    }

    // Wait for all task transitions to complete.
    countDownLatch.await();

    // Return an unguessable value.
    return System.currentTimeMillis() % 5 == 0;
  }
}
