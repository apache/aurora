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
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.inject.Singleton;

import com.google.common.base.Optional;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;

import org.apache.aurora.benchmark.fakes.FakeDriver;
import org.apache.aurora.benchmark.fakes.FakeOfferManager;
import org.apache.aurora.benchmark.fakes.FakeRescheduleCalculator;
import org.apache.aurora.benchmark.fakes.FakeSchedulerDriver;
import org.apache.aurora.benchmark.fakes.FakeStatsProvider;
import org.apache.aurora.common.application.ShutdownStage;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.base.Commands;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.scheduler.TaskIdGenerator;
import org.apache.aurora.scheduler.TaskStatusHandler;
import org.apache.aurora.scheduler.TaskStatusHandlerImpl;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.base.AsyncUtil;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.events.PubsubEvent;
import org.apache.aurora.scheduler.filter.SchedulingFilter;
import org.apache.aurora.scheduler.filter.SchedulingFilterImpl;
import org.apache.aurora.scheduler.mesos.Driver;
import org.apache.aurora.scheduler.mesos.DriverFactory;
import org.apache.aurora.scheduler.mesos.DriverSettings;
import org.apache.aurora.scheduler.mesos.FrameworkInfoFactory;
import org.apache.aurora.scheduler.mesos.MesosCallbackHandler;
import org.apache.aurora.scheduler.mesos.MesosCallbackHandler.MesosCallbackHandlerImpl;
import org.apache.aurora.scheduler.mesos.MesosSchedulerImpl;
import org.apache.aurora.scheduler.mesos.ProtosConversion;
import org.apache.aurora.scheduler.mesos.SchedulerDriverModule;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.aurora.scheduler.offers.OfferManager;
import org.apache.aurora.scheduler.preemptor.ClusterStateImpl;
import org.apache.aurora.scheduler.scheduling.RescheduleCalculator;
import org.apache.aurora.scheduler.state.StateModule;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.db.DbUtil;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.mesos.Scheduler;
import org.apache.mesos.v1.Protos;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.slf4j.LoggerFactory;

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

  private Scheduler scheduler;
  private AbstractExecutionThreadService statusHandler;
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
        new StateModule(new CliOptions()),
        new TierModule(TaskTestUtil.TIER_CONFIG),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Driver.class).toInstance(new FakeDriver());
            bind(Scheduler.class).to(MesosSchedulerImpl.class);
            bind(MesosSchedulerImpl.class).in(Singleton.class);
            bind(MesosCallbackHandler.class).to(MesosCallbackHandlerImpl.class);
            bind(MesosCallbackHandlerImpl.class).in(Singleton.class);
            bind(Executor.class)
                .annotatedWith(SchedulerDriverModule.SchedulerExecutor.class)
                .toInstance(AsyncUtil.singleThreadLoggingScheduledExecutor(
                    "SchedulerImpl-%d",
                    LoggerFactory.getLogger(StatusUpdateBenchmark.class)));
            bind(DriverFactory.class)
                .toInstance((s, credentials, frameworkInfo, master) -> new FakeSchedulerDriver());
            bind(OfferManager.class).toInstance(new FakeOfferManager());
            bind(TaskIdGenerator.class).to(TaskIdGenerator.TaskIdGeneratorImpl.class);
            bind(SchedulingFilter.class).to(SchedulingFilterImpl.class);
            bind(Command.class).annotatedWith(ShutdownStage.class).toInstance(Commands.NOOP);
            bind(Thread.UncaughtExceptionHandler.class).toInstance(
                (t, e) -> {
                  // no-op
                });
            bind(Storage.class).toInstance(storage);
            bind(DriverSettings.class).toInstance(
                new DriverSettings(
                    "fakemaster",
                    Optional.absent()));
            bind(FrameworkInfoFactory.class).toInstance(() -> Protos.FrameworkInfo.newBuilder()
                    .setUser("framework user")
                    .setName("test framework")
                    .build());
            bind(RescheduleCalculator.class).toInstance(new FakeRescheduleCalculator());
            bind(Clock.class).toInstance(new FakeClock());
            bind(ExecutorSettings.class).toInstance(TestExecutorSettings.THERMOS_EXECUTOR);
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(EventSink.class).toInstance(eventBus::post);
            bind(new TypeLiteral<BlockingQueue<Protos.TaskStatus>>() { })
                .annotatedWith(TaskStatusHandlerImpl.StatusUpdateQueue.class)
                .toInstance(new LinkedBlockingQueue<>());
            bind(new TypeLiteral<Integer>() { })
                .annotatedWith(TaskStatusHandlerImpl.MaxBatchSize.class)
                .toInstance(1000);
            bind(TaskStatusHandler.class).to(TaskStatusHandlerImpl.class);
            bind(TaskStatusHandlerImpl.class).in(Singleton.class);
            bind(IServerInfo.class).toInstance(IServerInfo.build(new ServerInfo("jmh", "")));
          }
        }
    );

    eventBus.register(injector.getInstance(ClusterStateImpl.class));
    scheduler = injector.getInstance(Scheduler.class);
    eventBus.register(this);

    statusHandler = injector.getInstance(TaskStatusHandlerImpl.class);
    statusHandler.startAsync();
  }

  @TearDown(Level.Trial)
  public void tearDown() {
    statusHandler.stopAsync();
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

    storage.write(
        (NoResult.Quiet) storeProvider -> storeProvider.getUnsafeTaskStore().saveTasks(tasks));

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

      scheduler.statusUpdate(new FakeSchedulerDriver(), ProtosConversion.convert(status));
    }

    // Wait for all task transitions to complete.
    countDownLatch.await();

    // Return an unguessable value.
    return System.currentTimeMillis() % 5 == 0;
  }
}
