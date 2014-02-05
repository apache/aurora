/**
 * Copyright 2013 Apache Software Foundation
 *
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
import java.io.IOException;
import java.io.InputStream;
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
import com.google.common.net.HostAndPort;
import com.google.common.testing.TearDown;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.twitter.common.application.Lifecycle;
import com.twitter.common.application.StartupStage;
import com.twitter.common.application.modules.AppLauncherModule;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.io.FileUtils;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ServerSetImpl;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule;
import com.twitter.common.zookeeper.guice.client.ZooKeeperClientModule.ClientConfig;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import org.apache.aurora.codec.ThriftBinaryCodec.CodingException;
import org.apache.aurora.gen.AssignedTask;
import org.apache.aurora.gen.Identity;
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
import org.apache.aurora.scheduler.DriverFactory;
import org.apache.aurora.scheduler.MesosTaskFactory.ExecutorConfig;
import org.apache.aurora.scheduler.configuration.ConfigurationManager;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.storage.backup.BackupModule;
import org.apache.aurora.scheduler.storage.log.LogManager.StreamManager.EntrySerializer;
import org.apache.aurora.scheduler.storage.log.LogStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotStoreImpl;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher;
import org.apache.aurora.scheduler.storage.log.testing.LogOpMatcher.StreamMatcher;
import org.apache.aurora.scheduler.thrift.ThriftConfiguration;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SchedulerIT extends BaseZooKeeperTest {

  private static final Logger LOG = Logger.getLogger(SchedulerIT.class.getName());

  private static final String CLUSTER_NAME = "integration_test_cluster";
  private static final String SERVERSET_PATH = "/fake/service/path";
  private static final String FRAMEWORK_ID = "integration_test_framework_id";

  private ExecutorService executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("SchedulerIT-%d").setDaemon(true).build());
  private AtomicReference<Optional<RuntimeException>> mainException =
      Atomics.newReference(Optional.<RuntimeException>absent());

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
    backupDir = FileUtils.createTempDir();
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
    entrySerializer = new EntrySerializer(LogStorageModule.MAX_LOG_ENTRY_SIZE.get());

    zkClient = createZkClient();

    Module testModule = new AbstractModule() {
      @Override
      protected void configure() {
        bind(DriverFactory.class).toInstance(driverFactory);
        bind(Log.class).toInstance(log);
        bind(ThriftConfiguration.class).toInstance(
            new ThriftConfiguration() {
              @Override
              public Optional<? extends InputStream> getSslKeyStream() throws IOException {
                return Optional.of(
                    com.google.common.io.Resources.getResource(getClass(), "AuroraTestKeyStore")
                        .openStream());
              }

              @Override
              public int getServingPort() {
                return 0;
              }
            }
        );
        bind(ExecutorConfig.class).toInstance(new ExecutorConfig("/executor/thermos"));
        install(new BackupModule(backupDir, SnapshotStoreImpl.class));
      }
    };

    ClientConfig zkClientConfig = ClientConfig
        .create(ImmutableList.of(InetSocketAddress.createUnresolved("localhost", getPort())))
        .withCredentials(ZooKeeperClient.digestCredentials("mesos", "mesos"));
    injector = Guice.createInjector(
        ImmutableList.<Module>builder()
            .addAll(SchedulerMain.getModules(CLUSTER_NAME, SERVERSET_PATH, zkClientConfig))
            .add(new LifecycleModule())
            .add(new AppLauncherModule())
            .add(new ZooKeeperClientModule(zkClientConfig))
            .add(testModule)
            .build()
    );
    lifecycle = injector.getInstance(Lifecycle.class);
  }

  private void startScheduler() throws Exception {
    injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class)).execute();

    // Mimic AppLauncher running main.
    final SchedulerMain main = injector.getInstance(SchedulerMain.class);
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
        new ExecutorServiceShutdown(executor, Amount.of(10L, Time.SECONDS)).execute();
      }
    });
  }

  private HostAndPort awaitSchedulerReady() throws Exception {
    return executor.submit(new Callable<HostAndPort>() {
      @Override
      public HostAndPort call() throws Exception {
        final AtomicReference<HostAndPort> thriftEndpoint = Atomics.newReference();
        ServerSet schedulerService = new ServerSetImpl(zkClient, SERVERSET_PATH);
        final CountDownLatch schedulerReady = new CountDownLatch(1);
        schedulerService.watch(new HostChangeMonitor<ServiceInstance>() {
          @Override
          public void onChange(ImmutableSet<ServiceInstance> hostSet) {
            if (!hostSet.isEmpty()) {
              Endpoint endpoint = Iterables.getOnlyElement(hostSet).getServiceEndpoint();
              thriftEndpoint.set(HostAndPort.fromParts(endpoint.getHost(), endpoint.getPort()));
              schedulerReady.countDown();
            }
          }
        });
        // A timeout is used because certain types of assertion errors (mocks) will not surface
        // until the main test thread exits this body of code.
        assertTrue(schedulerReady.await(5L, TimeUnit.MINUTES));
        return thriftEndpoint.get();
      }
    }).get();
  }

  private AtomicInteger curPosition = new AtomicInteger();
  private class IntPosition implements Position {
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
                  return entrySerializer.serialize(entry)[0];
                } catch (CodingException e) {
                  throw Throwables.propagate(e);
                }
              }
            };
          }
        });
  }

  private static ScheduledTask makeTask(String id, ScheduleStatus status) {
    ScheduledTask scheduledTask = new ScheduledTask()
        .setStatus(status)
        .setTaskEvents(ImmutableList.of(new TaskEvent(100, status)))
        .setAssignedTask(new AssignedTask()
            .setTaskId(id)
            .setTask(new TaskConfig()
                .setJobName("job-" + id)
                .setEnvironment("test")
                .setExecutorConfig(new org.apache.aurora.gen.ExecutorConfig("AuroraExecutor", ""))
                .setOwner(new Identity("role-" + id, "user-" + id))));
    // Apply defaults here so that we can expect the same task that will be written to the stream.
    ConfigurationManager.applyDefaultsIfUnset(scheduledTask.getAssignedTask().getTask());
    return scheduledTask;
  }

  @Test
  public void testLaunch() throws Exception {
    expect(driverFactory.apply(null)).andReturn(driver).anyTimes();

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
    streamMatcher.expectTransaction(Op.saveTasks(new SaveTasks(ImmutableSet.<ScheduledTask>of())))
        .andReturn(nextPosition());
    streamMatcher.expectTransaction(Op.saveFrameworkId(new SaveFrameworkId(FRAMEWORK_ID)))
        .andReturn(nextPosition());

    logStream.close();
    expectLastCall().anyTimes();

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
    injector.getInstance(Scheduler.class).registered(driver,
        FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build(),
        MasterInfo.getDefaultInstance());

    awaitSchedulerReady();

    assertEquals(0L, Stats.<Long>getVariable("task_store_PENDING").read().longValue());
    assertEquals(1L, Stats.<Long>getVariable("task_store_ASSIGNED").read().longValue());
    assertEquals(1L, Stats.<Long>getVariable("task_store_RUNNING").read().longValue());
    assertEquals(0L, Stats.<Long>getVariable("task_store_UNKNOWN").read().longValue());

    // TODO(William Farner): Send a thrift RPC to the scheduler.
    // TODO(William Farner): Also send an admin thrift RPC to verify capability (e.g. ROOT) mapping.
  }
}
