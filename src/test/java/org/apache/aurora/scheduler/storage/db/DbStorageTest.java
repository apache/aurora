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

import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.CronJobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.ibatis.exceptions.PersistenceException;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

public class DbStorageTest extends EasyMockTest {

  private SqlSessionFactory sessionFactory;
  private SqlSession session;
  private EnumValueMapper enumMapper;
  private Work.Quiet<String> readWork;
  private MutateWork.NoResult.Quiet writeWork;

  private DbStorage storage;

  @Before
  public void setUp() {
    sessionFactory = createMock(SqlSessionFactory.class);
    session = createMock(SqlSession.class);
    enumMapper = createMock(EnumValueMapper.class);
    readWork = createMock(new Clazz<Work.Quiet<String>>() { });
    writeWork = createMock(new Clazz<MutateWork.NoResult.Quiet>() { });

    storage = new DbStorage(
        sessionFactory,
        enumMapper,
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
    expect(readWork.apply(EasyMock.<StoreProvider>anyObject()))
        .andThrow(new PersistenceException());

    control.replay();

    storage.read(readWork);
  }

  @Test
  public void testRead() {
    expect(readWork.apply(EasyMock.<StoreProvider>anyObject())).andReturn("hi");

    control.replay();

    assertEquals("hi", storage.read(readWork));
  }

  @Test(expected = StorageException.class)
  public void testBulkLoadFails() {
    expect(sessionFactory.openSession(false)).andReturn(session);
    expect(session.update(DbStorage.DISABLE_UNDO_LOG)).andThrow(new PersistenceException());
    expect(session.update(DbStorage.ENABLE_UNDO_LOG)).andReturn(0);

    control.replay();

    storage.bulkLoad(writeWork);
  }

  @Test
  public void testBulkLoad() {
    expect(sessionFactory.openSession(false)).andReturn(session);
    expect(session.update(DbStorage.DISABLE_UNDO_LOG)).andReturn(0);
    writeWork.apply(EasyMock.<MutableStoreProvider>anyObject());
    session.close();
    expect(session.update(DbStorage.ENABLE_UNDO_LOG)).andReturn(0);

    control.replay();

    storage.bulkLoad(writeWork);
  }
}
