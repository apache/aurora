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
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;

import org.apache.aurora.GuavaUtils;
import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.common.application.Lifecycle;
import org.apache.aurora.common.stats.Stats;
import org.apache.aurora.common.zookeeper.Credentials;
import org.apache.aurora.common.zookeeper.ServerSetImpl;
import org.apache.aurora.common.zookeeper.ZooKeeperClient;
import org.apache.aurora.common.zookeeper.testing.BaseZooKeeperClientTest;
import org.apache.aurora.gen.HostAttributes;
import org.apache.aurora.gen.MaintenanceMode;
import org.apache.aurora.gen.ScheduleStatus;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.ServerInfo;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveFrameworkId;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.gen.storage.storageConstants;
import org.apache.aurora.scheduler.AppStartup;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.configuration.executor.ExecutorSettings;
import org.apache.aurora.scheduler.discovery.ServiceDiscoveryModule;
import org.apache.aurora.scheduler.discovery.ZooKeeperConfig;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.mesos.DriverFactory;
import org.apache.aurora.scheduler.mesos.DriverSettings;
import org.apache.aurora.scheduler.mesos.TestExecutorSettings;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.entities.IHostAttributes;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.apache.aurora.scheduler.storage.log.EntrySerializer;
import org.apache.aurora.scheduler.storage.log.LogStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher.StreamMatcher;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.aurora.common.testing.easymock.EasyMockTest.createCapture;
import static org.apache.aurora.scheduler.resources.ResourceTestUtil.mesosScalar;
import static org.apache.aurora.scheduler.resources.ResourceType.CPUS;
import static org.apache.aurora.scheduler.resources.ResourceType.RAM_MB;
import static org.apache.mesos.Protos.FrameworkInfo;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchedulerIT extends BaseZooKeeperClientTest {

  private static final Logger LOG = LoggerFactory.getLogger(SchedulerIT.class);

  private static final String CLUSTER_NAME = "integration_test_cluster";
  private static final String SERVERSET_PATH = "/fake/service/path";
  private static final String STATS_URL_PREFIX = "fake_url";
  private static final String FRAMEWORK_ID = "integration_test_framework_id";
  private static final IHostAttributes HOST_ATTRIBUTES = IHostAttributes.build(new HostAttributes()
      .setHost("host")
      .setSlaveId("slave-id")
      .setMode(MaintenanceMode.NONE)
      .setAttributes(ImmutableSet.of()));

  private static final DriverSettings SETTINGS = new DriverSettings(
      "fakemaster",
      Optional.absent(),
      FrameworkInfo.newBuilder()
          .setUser("framework user")
          .setName("test framework")
          .build());

  private final ExecutorService executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("SchedulerIT-%d").setDaemon(true).build());
  private final AtomicReference<Optional<RuntimeException>> mainException =
      Atomics.newReference(Optional.absent());

  private IMocksControl control;

  private SchedulerDriver driver;
  private DriverFactory driverFactory;
  private Log log;
  private Stream logStream;
  private StreamMatcher streamMatcher;
  private EntrySerializer entrySerializer;
  private ZooKeeperClient zkClient;
  private File backupDir;
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void mySetUp() throws Exception {
    control = createControl();
    addTearDown(() -> {
      if (mainException.get().isPresent()) {
        RuntimeException e = mainException.get().get();
        LOG.error("Scheduler main exited with an exception", e);
        fail(e.getMessage());
      }
      control.verify();
    });
    backupDir = temporaryFolder.newFolder();
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
        Set<Protos.Resource> overhead = ImmutableSet.of(
            mesosScalar(CPUS, 0.1),
            mesosScalar(RAM_MB, 1));
        bind(ExecutorSettings.class)
            .toInstance(TestExecutorSettings.thermosOnlyWithOverhead(overhead));
        install(new BackupModule(backupDir, SnapshotStoreImpl.class));

        bind(IServerInfo.class).toInstance(
            IServerInfo.build(
                new ServerInfo()
                    .setClusterName(CLUSTER_NAME)
                    .setStatsUrlPrefix(STATS_URL_PREFIX)));
      }
    };
    ZooKeeperConfig zkClientConfig =
        ZooKeeperConfig.create(
            true, // useCurator
            ImmutableList.of(InetSocketAddress.createUnresolved("localhost", getPort())))
            .withCredentials(Credentials.digestCredentials("mesos", "mesos"));
    SchedulerMain main = SchedulerMain.class.newInstance();
    Injector injector = Guice.createInjector(
        ImmutableList.<Module>builder()
            .add(SchedulerMain.getUniversalModule())
            .add(new TierModule(TaskTestUtil.TIER_CONFIG))
            .add(new LogStorageModule())
            .add(new ServiceDiscoveryModule(zkClientConfig, SERVERSET_PATH))
            .add(testModule)
            .build()
    );
    injector.injectMembers(main);
    Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    executor.submit(() -> {
      try {
        main.run();
      } catch (RuntimeException e) {
        mainException.set(Optional.of(e));
        executor.shutdownNow();
      }
    });
    addTearDown(() -> {
      lifecycle.shutdown();
      MoreExecutors.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    });
    injector.getInstance(Key.get(GuavaUtils.ServiceManagerIface.class, AppStartup.class))
        .awaitHealthy();
  }

  private void awaitSchedulerReady() throws Exception {
    executor.submit(() -> {
      ServerSetImpl schedulerService = new ServerSetImpl(zkClient, SERVERSET_PATH);
      final CountDownLatch schedulerReady = new CountDownLatch(1);
      schedulerService.watch(hostSet -> {
        if (!hostSet.isEmpty()) {
          schedulerReady.countDown();
        }
      });
      // A timeout is used because certain types of assertion errors (mocks) will not surface
      // until the main test thread exits this body of code.
      assertTrue(schedulerReady.await(5L, TimeUnit.MINUTES));
      return null;
    }).get();
  }

  private final AtomicInteger curPosition = new AtomicInteger();
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
        entry -> () -> {
          try {
            return Iterables.getFirst(entrySerializer.serialize(entry), null);
          } catch (CodingException e) {
            throw Throwables.propagate(e);
          }
        });
  }

  private static IScheduledTask makeTask(String id, ScheduleStatus status) {
    ScheduledTask builder = TaskTestUtil.addStateTransition(
        TaskTestUtil.makeTask(id, TaskTestUtil.JOB),
        status,
        100)
        .newBuilder();
    builder.getAssignedTask()
        .setSlaveId(HOST_ATTRIBUTES.getSlaveId())
        .setSlaveHost(HOST_ATTRIBUTES.getHost());
    return IScheduledTask.build(builder);
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

    IScheduledTask snapshotTask = makeTask("snapshotTask", ScheduleStatus.ASSIGNED);
    IScheduledTask transactionTask = makeTask("transactionTask", ScheduleStatus.RUNNING);
    Iterable<Entry> recoveredEntries = toEntries(
        LogEntry.snapshot(new Snapshot()
            .setTasks(ImmutableSet.of(snapshotTask.newBuilder()))
            .setHostAttributes(ImmutableSet.of(HOST_ATTRIBUTES.newBuilder()))),
        LogEntry.transaction(new Transaction(
            ImmutableList.of(Op.saveTasks(
                new SaveTasks(ImmutableSet.of(transactionTask.newBuilder())))),
            storageConstants.CURRENT_SCHEMA_VERSION)));

    expect(log.open()).andReturn(logStream);
    expect(logStream.readAll()).andReturn(recoveredEntries.iterator()).anyTimes();
    streamMatcher.expectTransaction(Op.saveFrameworkId(new SaveFrameworkId(FRAMEWORK_ID)))
        .andReturn(nextPosition());

    CountDownLatch driverStarted = new CountDownLatch(1);
    expect(driver.start()).andAnswer(() -> {
      driverStarted.countDown();
      return Status.DRIVER_RUNNING;
    });

    // Try to be a good test suite citizen by releasing the blocked thread when the test case exits.
    CountDownLatch testCompleted = new CountDownLatch(1);
    expect(driver.join()).andAnswer(() -> {
      testCompleted.await();
      return Status.DRIVER_STOPPED;
    });
    addTearDown(testCompleted::countDown);
    expect(driver.stop(true)).andReturn(Status.DRIVER_STOPPED).anyTimes();

    control.replay();
    startScheduler();

    driverStarted.await();
    scheduler.getValue().registered(driver,
        FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build(),
        MasterInfo.getDefaultInstance());

    awaitSchedulerReady();

    assertEquals(0L, Stats.<Long>getVariable("task_store_PENDING").read().longValue());
    assertEquals(1L, Stats.<Long>getVariable("task_store_ASSIGNED").read().longValue());
    assertEquals(1L, Stats.<Long>getVariable("task_store_RUNNING").read().longValue());

    // TODO(William Farner): Send a thrift RPC to the scheduler.
    // TODO(William Farner): Also send an admin thrift RPC to verify capability (e.g. ROOT) mapping.
  }
}
