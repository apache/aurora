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
package org.apache.aurora.scheduler.app;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.application.StartupStage;
import org.apache.aurora.common.application.modules.AppLauncherModule;
import org.apache.aurora.common.application.modules.LifecycleModule;
import org.apache.aurora.common.base.ExceptionalCommand;
import org.apache.aurora.common.net.pool.DynamicHostSet.HostChangeMonitor;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Data;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.thrift.ServiceInstance;
import org.apache.aurora.common.zookeeper.ServerSet;
import org.apache.aurora.common.zookeeper.ServerSetImpl;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.aurora.common.zookeeper.guice.client.ZooKeeperClientModule;
import org.apache.aurora.common.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;
import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperTest;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.JobKey;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskEvent;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.gen.storage.storageConstants;
import org.apache.aurora.scheduler.ResourceSlot;
import org.apache.aurora.scheduler.async.AsyncModule.AsyncExecutor;
import org.apache.aurora.scheduler.async.FlushableWorkQueue;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.mesos.DriverFactory;
import org.apache.aurora.scheduler.mesos.DriverSettings;
import org.apache.aurora.scheduler.mesos.ExecutorSettings;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.log.EntrySerializer;
import org.apache.aurora.scheduler.storage.log.LogStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher.StreamMatcher;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.common.testing.easymock.EasyMockTest.createCapture;
import static org.apache.mesos.Protos.FrameworkInfo;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchedulerIT extends BaseZooKeeperTest {

  private static final Logger LOG = Logger.getLogger(SchedulerIT.class.getName());

  private static final String CLUSTER_NAME = "integration_test_cluster";
  private static final String SERVERSET_PATH = "/fake/service/path";
  private static final String STATS_URL_PREFIX = "fake_url";
  private static final String FRAMEWORK_ID = "integration_test_framework_id";

  private static final DriverSettings SETTINGS = new DriverSettings(
      "fakemaster",
      Optional.absent(),
      FrameworkInfo.newBuilder()
          .setUser("framework user")
          .setName("test framework")
          .build());

  private ExecutorService executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("SchedulerIT-%d").setDaemon(true).build());
  private AtomicReference<Optional<RuntimeException>> mainException =
      Atomics.newReference(Optional.absent());

  private IMocksControl control;
  private Injector injector;

  private SchedulerDriver driver;
  private DriverFactory driverFactory;
  private Log log;
  private Stream logStream;
  private StreamMatcher streamMatcher;
  private EntrySerializer entrySerializer;
  private ZooKeeperClient zkClient;
  private File backupDir;
  private Lifecycle lifecycle;

  @Before
  public void mySetUp() throws Exception {
    control = createControl();
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        if (mainException.get().isPresent()) {
          RuntimeException e = mainException.get().get();
          LOG.log(Level.SEVERE, "Scheduler main exited with an exception", e);
          fail(e.getMessage());
        }
        control.verify();
      }
    });
    backupDir = Files.createTempDir();
    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        org.apache.commons.io.FileUtils.deleteDirectory(backupDir);
      }
    });

    driver = control.createMock(SchedulerDriver.class);
    // This is necessary to allow driver to block, otherwise it would stall other mocks.
    EasyMock.makeThreadSafe(driver, false);

    driverFactory = control.createMock(DriverFactory.class);
    log = control.createMock(Log.class);
    logStream = control.createMock(Stream.class);
    streamMatcher = LogOpMatcher.matcherFor(logStream);
    entrySerializer = new EntrySerializer.EntrySerializerImpl(
        LogStorageModule.MAX_LOG_ENTRY_SIZE.get(),
        Hashing.md5());

    zkClient = createZkClient();
  }

  private void startScheduler() throws Exception {
    // TODO(wfarner): Try to accomplish all this by subclassing SchedulerMain and actually using
    // AppLauncher.
    Module testModule = new AbstractModule() {
      @Override
      protected void configure() {
        bind(DriverFactory.class).toInstance(driverFactory);
        bind(DriverSettings.class).toInstance(SETTINGS);
        bind(Log.class).toInstance(log);
        ResourceSlot executorOverhead = new ResourceSlot(
            0.1,
            Amount.of(1L, Data.MB),
            Amount.of(0L, Data.MB),
            0);
        bind(ExecutorSettings.class)
            .toInstance(ExecutorSettings.newBuilder()
                .setExecutorPath("/executor/thermos")
                .setThermosObserverRoot("/var/run/thermos")
                .setExecutorOverhead(executorOverhead)
                .build());
        install(new BackupModule(backupDir, SnapshotStoreImpl.class));
      }
    };
    ClientConfig zkClientConfig = ClientConfig
        .create(ImmutableList.of(InetSocketAddress.createUnresolved("localhost", getPort())))
        .withCredentials(ZooKeeperClient.digestCredentials("mesos", "mesos"));
    final SchedulerMain main = SchedulerMain.class.newInstance();
    injector = Guice.createInjector(
        ImmutableList.<Module>builder()
            .addAll(main.getModules(CLUSTER_NAME, SERVERSET_PATH, zkClientConfig, STATS_URL_PREFIX))
            .add(new LifecycleModule())
            .add(new AppLauncherModule())
            .add(new ZooKeeperClientModule(zkClientConfig))
            .add(testModule)
            .build()
    );
    injector.injectMembers(main);
    lifecycle = injector.getInstance(Lifecycle.class);

    injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class)).execute();

    // Mimic AppLauncher running main.
    executor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          main.run();
        } catch (RuntimeException e) {
          mainException.set(Optional.of(e));
          executor.shutdownNow();
        }
      }
    });
    addTearDown(new TearDown() {
      @Override
      public void tearDown() throws Exception {
        lifecycle.shutdown();
        MoreExecutors.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
      }
    });
  }

  private void awaitSchedulerReady() throws Exception {
    executor.submit(new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        ServerSet schedulerService = new ServerSetImpl(zkClient, SERVERSET_PATH);
        final CountDownLatch schedulerReady = new CountDownLatch(1);
        schedulerService.watch(new HostChangeMonitor<ServiceInstance>() {
          @Override
          public void onChange(ImmutableSet<ServiceInstance> hostSet) {
            if (!hostSet.isEmpty()) {
              schedulerReady.countDown();
            }
          }
        });
        // A timeout is used because certain types of assertion errors (mocks) will not surface
        // until the main test thread exits this body of code.
        assertTrue(schedulerReady.await(5L, TimeUnit.MINUTES));
        return null;
      }
    }).get();
  }

  private AtomicInteger curPosition = new AtomicInteger();
  private static class IntPosition implements Position {
    private final int pos;

    IntPosition(int pos) {
      this.pos = pos;
    }

    @Override
    public int compareTo(Position position) {
      return pos - ((IntPosition) position).pos;
    }
  }
  private IntPosition nextPosition() {
    return new IntPosition(curPosition.incrementAndGet());
  }

  private Iterable<Entry> toEntries(LogEntry... entries) {
    return Iterables.transform(Arrays.asList(entries),
        new Function<LogEntry, Entry>() {
          @Override
          public Entry apply(final LogEntry entry) {
            return new Entry() {
              @Override
              public byte[] contents() {
                try {
                  return Iterables.getFirst(entrySerializer.serialize(entry), null);
                } catch (CodingException e) {
                  throw Throwables.propagate(e);
                }
              }
            };
          }
        });
  }

  private static ScheduledTask makeTask(String id, ScheduleStatus status) {
    return new ScheduledTask()
        .setStatus(status)
        .setTaskEvents(ImmutableList.of(new TaskEvent(100, status)))
        .setAssignedTask(new AssignedTask()
            .setSlaveId("slaveId")
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setJob(new JobKey("role-" + id, "test", "job-" + id))
                .setJobName("job-" + id)
                .setEnvironment("test")
                .setExecutorConfig(new org.apache.aurora.gen.ExecutorConfig("AuroraExecutor", ""))
                .setOwner(new Identity("role-" + id, "user-" + id))));
  }

  @Test
  public void testLaunch() throws Exception {
    Capture<Scheduler> scheduler = createCapture();
    expect(driverFactory.create(
        capture(scheduler),
        eq(SETTINGS.getCredentials()),
        eq(SETTINGS.getFrameworkInfo()),
        eq(SETTINGS.getMasterUri())))
        .andReturn(driver).anyTimes();

    ScheduledTask snapshotTask = makeTask("snapshotTask", ScheduleStatus.ASSIGNED);
    ScheduledTask transactionTask = makeTask("transactionTask", ScheduleStatus.RUNNING);
    Iterable<Entry> recoveredEntries = toEntries(
        LogEntry.snapshot(new Snapshot().setTasks(ImmutableSet.of(snapshotTask))),
        LogEntry.transaction(new Transaction(
            ImmutableList.of(Op.saveTasks(new SaveTasks(ImmutableSet.of(transactionTask)))),
            storageConstants.CURRENT_SCHEMA_VERSION)));

    expect(log.open()).andReturn(logStream);
    expect(logStream.readAll()).andReturn(recoveredEntries.iterator()).anyTimes();
    // An empty saveTasks is an artifact of the fact that mutateTasks always writes a log operation
    // even if nothing is changed.
    streamMatcher.expectTransaction(Op.saveTasks(new SaveTasks(ImmutableSet.of())))
        .andReturn(nextPosition());
    streamMatcher.expectTransaction(Op.saveFrameworkId(new SaveFrameworkId(FRAMEWORK_ID)))
        .andReturn(nextPosition());

    final CountDownLatch driverStarted = new CountDownLatch(1);
    expect(driver.start()).andAnswer(new IAnswer<Status>() {
      @Override
      public Status answer() {
        driverStarted.countDown();
        return Status.DRIVER_RUNNING;
      }
    });

    // Try to be a good test suite citizen by releasing the blocked thread when the test case exits.
    final CountDownLatch testCompleted = new CountDownLatch(1);
    expect(driver.join()).andAnswer(new IAnswer<Status>() {
      @Override
      public Status answer() throws Throwable {
        testCompleted.await();
        return Status.DRIVER_STOPPED;
      }
    });
    addTearDown(new TearDown() {
      @Override
      public void tearDown() {
        testCompleted.countDown();
      }
    });
    expect(driver.stop(true)).andReturn(Status.DRIVER_STOPPED).anyTimes();

    control.replay();
    startScheduler();

    driverStarted.await();
    scheduler.getValue().registered(driver,
        FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build(),
        MasterInfo.getDefaultInstance());
    // Registration is published on the event bus, which will be gated until a flush.
    injector.getInstance(Key.get(FlushableWorkQueue.class, AsyncExecutor.class)).flush();

    awaitSchedulerReady();

    assertEquals(0L, Stats.<Long>getVariable("task_store_PENDING").read().longValue());
    assertEquals(1L, Stats.<Long>getVariable("task_store_ASSIGNED").read().longValue());
    assertEquals(1L, Stats.<Long>getVariable("task_store_RUNNING").read().longValue());

    // TODO(William Farner): Send a thrift RPC to the scheduler.
    // TODO(William Farner): Also send an admin thrift RPC to verify capability (e.g. ROOT) mapping.
  }
}
