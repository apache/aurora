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
package org.apache.aurora.scheduler.storage.log;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;

import org.apache.aurora.GuavaUtils.ServiceManagerIface;
import org.apache.aurora.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import org.apache.aurora.common.application.ShutdownStage;
import org.apache.aurora.common.base.Command;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.quantity.Amount;
import org.apache.aurora.common.quantity.Time;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.scheduler.SchedulerLifecycle.SchedulerActive;
import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.config.types.TimeAmount;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Position;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.storage.SnapshotStore;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.durability.DurableStorageModule;
import org.apache.aurora.scheduler.storage.log.SnapshotModule.Options;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.easymock.IAnswer;
import org.junit.Test;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;

public class SnapshotServiceTest extends EasyMockTest {

  private static final Snapshot SNAPSHOT = new Snapshot().setTasks(
      ImmutableSet.of(TaskTestUtil.makeTask("a", TaskTestUtil.JOB).newBuilder()));

  private NonVolatileStorage storage;
  private SnapshotStore snapshotStore;
  private ServiceManagerIface serviceManager;

  private Snapshotter mockSnapshotter;
  private Log mockLog;
  private Stream mockStream;
  private Position mockPosition;

  private void setUp(Amount<Long, Time> snapshotInterval) {
    mockSnapshotter = createMock(Snapshotter.class);
    mockLog = createMock(Log.class);
    mockStream = createMock(Stream.class);
    mockPosition = createMock(Position.class);

    Options options = new Options();
    options.snapshotInterval =
        new TimeAmount(snapshotInterval.getValue(), snapshotInterval.getUnit());

    Injector injector = Guice.createInjector(
        new SchedulerServicesModule(),
        new LogPersistenceModule(new LogPersistenceModule.Options()),
        new SnapshotModule(options),
        new DurableStorageModule(),
        new MemStorageModule(Bindings.annotatedKeyFactory(Volatile.class)),
        new TierModule(TaskTestUtil.TIER_CONFIG),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(Key.get(Command.class, ShutdownStage.class)).to(ShutdownRegistryImpl.class);
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(EventSink.class).toInstance(e -> { });
            bind(Snapshotter.class).toInstance(mockSnapshotter);
            bind(Log.class).toInstance(mockLog);
          }
        }
    );

    storage = injector.getInstance(NonVolatileStorage.class);
    snapshotStore = injector.getInstance(SnapshotStore.class);
    serviceManager =
        injector.getInstance(Key.get(ServiceManagerIface.class, SchedulerActive.class));
  }

  private void expectStorageInitialized() throws Exception {
    expect(mockLog.open()).andReturn(mockStream);
    List<Entry> empty = ImmutableList.of();
    expect(mockStream.readAll()).andReturn(empty.iterator());
  }

  private void expectSnapshotPersist(CountDownLatch latch) {
    expect(mockStream.append(anyObject())).andReturn(mockPosition).atLeastOnce();
    mockStream.truncateBefore(mockPosition);
    expectLastCall().andAnswer((IAnswer<Void>) () -> {
      latch.countDown();
      return null;
    }).atLeastOnce();
  }

  @Test
  public void testPeriodicSnapshots() throws Exception {
    setUp(Amount.of(1L, Time.MILLISECONDS));

    expectStorageInitialized();

    expect(mockSnapshotter.from(anyObject())).andReturn(SNAPSHOT).atLeastOnce();

    CountDownLatch snapshotCalled = new CountDownLatch(2);
    expectSnapshotPersist(snapshotCalled);

    control.replay();

    storage.prepare();
    storage.start(stores -> { });
    serviceManager.startAsync().awaitHealthy();

    snapshotCalled.await();

    serviceManager.stopAsync().awaitStopped(10, TimeUnit.SECONDS);
  }

  @Test
  public void testExplicitInternalSnapshot() throws Exception {
    setUp(Amount.of(1L, Time.HOURS));

    expectStorageInitialized();

    expect(mockSnapshotter.from(anyObject())).andReturn(SNAPSHOT);
    expectSnapshotPersist(new CountDownLatch(1));

    control.replay();

    storage.prepare();
    storage.start(stores -> { });
    snapshotStore.snapshot();
  }

  @Test
  public void testExplicitProvidedSnapshot() throws Exception {
    setUp(Amount.of(1L, Time.HOURS));

    expectStorageInitialized();
    expectSnapshotPersist(new CountDownLatch(1));

    control.replay();

    storage.prepare();
    storage.start(stores -> { });
    snapshotStore.snapshotWith(SNAPSHOT);
  }
}
