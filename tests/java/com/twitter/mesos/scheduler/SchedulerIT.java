package com.twitter.mesos.scheduler;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Optional;
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

import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.MasterInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;
import org.easymock.IAnswer;
import org.easymock.IMocksControl;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.twitter.common.application.ShutdownStage;
import com.twitter.common.application.StartupStage;
import com.twitter.common.application.modules.AppLauncherModule;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.net.pool.DynamicHostSet.HostChangeMonitor;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.concurrent.ExecutorServiceShutdown;
import com.twitter.common.zookeeper.ServerSet;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;
import com.twitter.common_internal.zookeeper.TwitterServerSet;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.LogEntry;
import com.twitter.mesos.gen.storage.Op;
import com.twitter.mesos.gen.storage.SaveFrameworkId;
import com.twitter.mesos.gen.storage.SaveTasks;
import com.twitter.mesos.gen.storage.Transaction;
import com.twitter.mesos.scheduler.MesosTaskFactory.MesosTaskFactoryImpl.ExecutorConfig;
import com.twitter.mesos.scheduler.SchedulerLifecycle.ShutdownOnDriverExit;
import com.twitter.mesos.scheduler.SchedulerModule.AuthMode;
import com.twitter.mesos.scheduler.ThriftServerLauncher.ThriftConfiguration;
import com.twitter.mesos.scheduler.log.Log;
import com.twitter.mesos.scheduler.log.Log.Entry;
import com.twitter.mesos.scheduler.log.Log.Position;
import com.twitter.mesos.scheduler.log.Log.Stream;
import com.twitter.mesos.scheduler.storage.log.LogManager.StreamManager.EntrySerializer;
import com.twitter.mesos.scheduler.storage.log.LogStorageModule;
import com.twitter.thrift.Endpoint;
import com.twitter.thrift.ServiceInstance;

import static org.easymock.EasyMock.aryEq;
import static org.easymock.EasyMock.createControl;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

@Ignore("TODO(William Farner): Fix this and close MESOS-1657")
public class SchedulerIT extends BaseZooKeeperTest {

  private static final String CLUSTER_NAME = "integration_test_cluster";
  private static final String FRAMEWORK_ID = "integration_test_framework_id";

  private ExecutorService executor = Executors.newCachedThreadPool(
      new ThreadFactoryBuilder().setNameFormat("SchedulerMain-%d").setDaemon(true).build());

  private IMocksControl control;
  private Injector injector;

  private SchedulerDriver driver;
  private DriverFactory driverFactory;
  private Log log;
  private Stream logStream;
  private EntrySerializer entrySerializer;
  private ZooKeeperClient zkClient;
  private Command shutdown;

  @Before
  public void mySetUp() throws Exception {
    control = createControl();
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        control.verify();
      }
    });

    driver = control.createMock(SchedulerDriver.class);
    driverFactory = control.createMock(DriverFactory.class);
    log = control.createMock(Log.class);
    logStream = control.createMock(Stream.class);
    entrySerializer = new EntrySerializer(LogStorageModule.MAX_LOG_ENTRY_SIZE.get());

    zkClient = createZkClient();

    Module testModule = new AbstractModule() {
      @Override protected void configure() {
        bind(DriverFactory.class).toInstance(driverFactory);
        bind(Log.class).toInstance(log);
        bind(ThriftConfiguration.class).toInstance(
            new ThriftConfiguration() {
              @Override public InputStream getSslKeyStream() throws IOException {
                return com.google.common.io.Resources.getResource(getClass(), "MesosTestKeyStore")
                    .openStream();
              }

              @Override public int getServingPort() {
                return 0;
              }
            }
        );
        bind(ExecutorConfig.class).toInstance(
            new ExecutorConfig("/executor/old", "/executor/thermos"));
        bind(Boolean.class).annotatedWith(ShutdownOnDriverExit.class).toInstance(false);
      }
    };

    injector = Guice.createInjector(
        SchedulerMain.getModules(
            CLUSTER_NAME,
            AuthMode.UNSECURE,
            Optional.of(InetSocketAddress.createUnresolved("localhost", getPort())),
            testModule,
            new LifecycleModule(),
            new AppLauncherModule()));
    shutdown = injector.getInstance(Key.get(Command.class, ShutdownStage.class));
  }

  private void startScheduler() throws Exception {
    final ExceptionalCommand<?> startup =
        injector.getInstance(Key.get(ExceptionalCommand.class, StartupStage.class));
    startup.execute();

    // Mimic AppLauncher running main.
    final SchedulerMain main = injector.getInstance(SchedulerMain.class);
    final Future<?> mainFuture = executor.submit(new Runnable() {
      @Override public void run() {
        main.run();
      }
    });
    addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        shutdown.execute();
        new ExecutorServiceShutdown(executor, Amount.of(1L, Time.SECONDS)).execute();
        mainFuture.get();
      }
    });
  }

  private Scheduler getScheduler() {
    return injector.getInstance(Scheduler.class);
  }

  private HostAndPort awaitSchedulerReady() throws Exception {
    final CountDownLatch schedulerReady = new CountDownLatch(1);
    final AtomicReference<HostAndPort> thriftEndpoint = Atomics.newReference();
    ServerSet schedulerService =
        TwitterServerSet.create(zkClient, SchedulerMain.createService(CLUSTER_NAME));
    schedulerService.monitor(new HostChangeMonitor<ServiceInstance>() {
      @Override public void onChange(ImmutableSet<ServiceInstance> hostSet) {
        if (!hostSet.isEmpty()) {
          Endpoint endpoint = Iterables.getOnlyElement(hostSet).getServiceEndpoint();
          thriftEndpoint.set(HostAndPort.fromParts(endpoint.getHost(), endpoint.getPort()));
          schedulerReady.countDown();
        }
      }
    });

    schedulerReady.await();
    return thriftEndpoint.get();
  }

  private AtomicInteger curPosition = new AtomicInteger();
  private class IntPosition implements Position {
    private final int pos;

    IntPosition(int pos) {
      this.pos = pos;
    }

    @Override public int compareTo(Position position) {
      return pos - ((IntPosition) position).pos;
    }
  }
  private IntPosition nextPosition() {
    return new IntPosition(curPosition.incrementAndGet());
  }

  private void expectLogWrite(LogEntry entry) throws Exception {
    for (byte[] chunk : entrySerializer.serialize(entry)) {
      expect(logStream.append(aryEq(chunk))).andReturn(nextPosition());
    }
  }

  private void expectLogOp(Op op) throws Exception {
    expectLogWrite(LogEntry.transaction(new Transaction().setOps(ImmutableList.of(op))));
  }

  private void expectSaveTasks(SaveTasks saveTasks) throws Exception {
    expectLogOp(Op.saveTasks(saveTasks));
  }

  @Test
  public void testLaunch() throws Exception {
    expect(driverFactory.apply(null)).andReturn(driver).anyTimes();

    expect(log.open()).andReturn(logStream);
    logStream.close();
    expectLastCall().anyTimes();

    Iterable<Entry> recoveredEntries = ImmutableList.of();
    expect(logStream.readAll()).andReturn(recoveredEntries.iterator()).anyTimes();
    expectSaveTasks(new SaveTasks().setTasks(ImmutableSet.<ScheduledTask>of()));
    expectLogOp(Op.saveFrameworkId(new SaveFrameworkId(FRAMEWORK_ID)));

    expect(driver.run()).andAnswer(new IAnswer<Status>() {
      @Override public Status answer() throws InterruptedException {
        return Status.DRIVER_RUNNING;
      }
    });

    control.replay();
    startScheduler();

    awaitSchedulerReady();

    Scheduler scheduler = getScheduler();
    scheduler.registered(driver,
        FrameworkID.newBuilder().setValue(FRAMEWORK_ID).build(),
        MasterInfo.getDefaultInstance());

    // TODO(William Farner): Send a thrift RPC to the scheduler.
  }
}
