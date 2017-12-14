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
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

import org.apache.aurora.codec.ThriftBinaryCodec;
import org.apache.aurora.common.inject.Bindings;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.common.util.BuildInfo;
import org.apache.aurora.common.util.Clock;
import org.apache.aurora.common.util.testing.FakeBuildInfo;
import org.apache.aurora.common.util.testing.FakeClock;
import org.apache.aurora.gen.storage.LogEntry;
import org.apache.aurora.gen.storage.Op;
import org.apache.aurora.gen.storage.SaveTasks;
import org.apache.aurora.gen.storage.Snapshot;
import org.apache.aurora.gen.storage.Transaction;
import org.apache.aurora.scheduler.TierModule;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.events.EventSink;
import org.apache.aurora.scheduler.log.Log;
import org.apache.aurora.scheduler.log.Log.Entry;
import org.apache.aurora.scheduler.log.Log.Stream;
import org.apache.aurora.scheduler.storage.Snapshotter;
import org.apache.aurora.scheduler.storage.Storage.Volatile;
import org.apache.aurora.scheduler.storage.durability.Persistence;
import org.apache.aurora.scheduler.storage.durability.Persistence.Edit;
import org.apache.aurora.scheduler.storage.log.LogStorageModule.Options;
import org.apache.aurora.scheduler.storage.mem.MemStorageModule;
import org.apache.aurora.scheduler.testing.FakeStatsProvider;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class LogPersistenceTest extends EasyMockTest {

  private Persistence persistence;

  private Log mockLog;
  private Stream mockStream;

  @Before
  public void setUp() {
    mockLog = createMock(Log.class);
    mockStream = createMock(Stream.class);

    Injector injector = Guice.createInjector(
        new LogStorageModule(new Options()),
        new MemStorageModule(Bindings.annotatedKeyFactory(Volatile.class)),
        new TierModule(TaskTestUtil.TIER_CONFIG),
        new AbstractModule() {
          @Override
          protected void configure() {
            bind(StatsProvider.class).toInstance(new FakeStatsProvider());
            bind(EventSink.class).toInstance(e -> { });
            bind(BuildInfo.class).toInstance(FakeBuildInfo.generateBuildInfo());
            bind(Clock.class).toInstance(new FakeClock());
            bind(Snapshotter.class).to(SnapshotStoreImpl.class);
            bind(Log.class).toInstance(mockLog);
          }
        }
    );

    persistence = injector.getInstance(Persistence.class);
  }

  @Test
  public void testRecoverEmpty() throws Exception {
    expect(mockLog.open()).andReturn(mockStream);
    List<Entry> empty = ImmutableList.of();
    expect(mockStream.readAll()).andReturn(empty.iterator());

    control.replay();

    persistence.prepare();
    assertEquals(ImmutableList.of(), persistence.recover().collect(Collectors.toList()));
  }

  @Test
  public void testRecoverSnapshot() throws Exception {
    expect(mockLog.open()).andReturn(mockStream);

    Op saveA = Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(
        TaskTestUtil.makeTask("a", TaskTestUtil.JOB).newBuilder())));
    Op saveB = Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(
        TaskTestUtil.makeTask("b", TaskTestUtil.JOB).newBuilder())));
    Op saveC = Op.saveTasks(new SaveTasks().setTasks(ImmutableSet.of(
        TaskTestUtil.makeTask("c", TaskTestUtil.JOB).newBuilder())));

    List<Entry> entries = ImmutableList.of(
        logEntry(LogEntry.transaction(new Transaction().setOps(ImmutableList.of(saveA)))),
        logEntry(LogEntry.snapshot(new Snapshot().setTasks(saveB.getSaveTasks().getTasks()))),
        logEntry(LogEntry.transaction(new Transaction().setOps(ImmutableList.of(saveC)))));

    expect(mockStream.readAll()).andReturn(entries.iterator());

    control.replay();

    persistence.prepare();
    assertEquals(
        ImmutableList.of(
            Edit.op(saveA),
            Edit.deleteAll(),
            Edit.op(saveB),
            Edit.op(saveC)),
        persistence.recover().collect(Collectors.toList()));
  }

  private static Entry logEntry(LogEntry entry) {
    return () -> ThriftBinaryCodec.encodeNonNull(entry);
  }
}
