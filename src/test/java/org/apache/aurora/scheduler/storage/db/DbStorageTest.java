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

import java.util.concurrent.atomic.AtomicBoolean;

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.async.FlushableWorkQueue;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class DbStorageTest extends EasyMockTest {

  private SqlSessionFactory sessionFactory;
  private SqlSession session;
  private EnumValueMapper enumMapper;
  private FlushableWorkQueue flusher;
  private Work.Quiet<String> readWork;
  private MutateWork.NoResult.Quiet writeWork;

  private DbStorage storage;

  @Before
  public void setUp() {
    sessionFactory = createMock(SqlSessionFactory.class);
    session = createMock(SqlSession.class);
    enumMapper = createMock(EnumValueMapper.class);
    flusher = createMock(FlushableWorkQueue.class);
    readWork = createMock(new Clazz<Work.Quiet<String>>() { });
    writeWork = createMock(new Clazz<MutateWork.NoResult.Quiet>() { });

    storage = new DbStorage(
        sessionFactory,
        enumMapper,
        flusher,
        createMock(CronJobStore.Mutable.class),
        createMock(TaskStore.Mutable.class),
        createMock(SchedulerStore.Mutable.class),
        createMock(AttributeStore.Mutable.class),
        createMock(LockStore.Mutable.class),
        createMock(QuotaStore.Mutable.class),
        createMock(JobUpdateStore.Mutable.class));
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

  @Test(expected = StorageException.class)
  public void testBulkLoadFails() {
    expect(sessionFactory.openSession(false)).andReturn(session);
    expect(session.update(DbStorage.DISABLE_UNDO_LOG)).andThrow(new PersistenceException());
    expect(session.update(DbStorage.ENABLE_UNDO_LOG)).andReturn(0);
    flusher.flush();

    control.replay();

    storage.bulkLoad(writeWork);
  }

  @Test
  public void testBulkLoad() {
    expect(sessionFactory.openSession(false)).andReturn(session);
    expect(session.update(DbStorage.DISABLE_UNDO_LOG)).andReturn(0);
    expect(writeWork.apply(EasyMock.anyObject())).andReturn(null);
    session.close();
    expect(session.update(DbStorage.ENABLE_UNDO_LOG)).andReturn(0);
    flusher.flush();

    control.replay();

    storage.bulkLoad(writeWork);
  }

  @Test
  public void testFlushWithReentrantWrites() {
    final AtomicBoolean flushed = new AtomicBoolean(false);
    flusher.flush();
    expectLastCall().andAnswer(new IAnswer<Void>() {
      @Override
      public Void answer() {
        flushed.set(true);
        return null;
      }
    });

    control.replay();

    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        noopWrite();

        // Should not have flushed yet.
        assertFalse("flush() should not be called until outer write() completes.", flushed.get());
      }
    });
  }

  private void noopWrite() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        // No-op.
      }
    });
  }

  @Test
  public void testFlushWithSeqentialWrites() {
    flusher.flush();
    expectLastCall().times(2);

    control.replay();

    noopWrite();
    noopWrite();
  }
}
