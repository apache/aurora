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
package org.apache.aurora.scheduler.storage.db;

import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.common.testing.easymock.EasyMockTest;
import org.apache.aurora.scheduler.async.GatedWorkQueue;
import org.apache.aurora.scheduler.async.GatedWorkQueue.GatedOperation;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSessionFactory;
import org.easymock.EasyMock;
import org.easymock.IExpectationSetters;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class DbStorageTest extends EasyMockTest {
  private GatedWorkQueue gatedWorkQueue;
  private Work.Quiet<String> readWork;

  private DbStorage storage;

  @Before
  public void setUp() {
    gatedWorkQueue = createMock(GatedWorkQueue.class);
    readWork = createMock(new Clazz<Work.Quiet<String>>() { });
    StatsProvider statsProvider = createMock(StatsProvider.class);

    storage = new DbStorage(
        createMock(SqlSessionFactory.class),
        createMock(EnumBackfill.class),
        gatedWorkQueue,
        createMock(CronJobStore.Mutable.class),
        createMock(TaskStore.Mutable.class),
        createMock(SchedulerStore.Mutable.class),
        createMock(AttributeStore.Mutable.class),
        createMock(LockStore.Mutable.class),
        createMock(QuotaStore.Mutable.class),
        createMock(JobUpdateStore.Mutable.class),
        statsProvider);
  }

  @Test(expected = StorageException.class)
  public void testReadFails() {
    expect(readWork.apply(EasyMock.anyObject()))
        .andThrow(new PersistenceException());

    control.replay();

    storage.read(readWork);
  }

  @Test
  public void testRead() {
    expect(readWork.apply(EasyMock.anyObject())).andReturn("hi");

    control.replay();

    assertEquals("hi", storage.read(readWork));
  }

  private IExpectationSetters<?> expectGateClosed() throws Exception {
    return expect(gatedWorkQueue.closeDuring(EasyMock.anyObject()))
        .andAnswer(() -> {
          GatedOperation<?, ?> op = (GatedOperation<?, ?>) EasyMock.getCurrentArguments()[0];
          return op.doWithGateClosed();
        });
  }

  @Test
  public void testGateWithReentrantWrites() throws Exception {
    expectGateClosed().times(2);

    control.replay();

    storage.write((NoResult.Quiet) storeProvider -> noopWrite());
  }

  private void noopWrite() {
    storage.write((NoResult.Quiet) storeProvider -> {
      // No-op.
    });
  }

  @Test
  public void testFlushWithSeqentialWrites() throws Exception {
    expectGateClosed().times(2);

    control.replay();

    noopWrite();
    noopWrite();
  }
}
