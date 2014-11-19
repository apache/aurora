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
package org.apache.aurora.scheduler.storage.testing;

import com.google.common.collect.ImmutableSet;
import com.twitter.common.testing.easymock.EasyMockTest;

import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.storage.AttributeStore;
import org.apache.aurora.scheduler.storage.JobStore;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.LockStore;
import org.apache.aurora.scheduler.storage.QuotaStore;
import org.apache.aurora.scheduler.storage.SchedulerStore;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.NonVolatileStorage;
import org.apache.aurora.scheduler.storage.Storage.StoreProvider;
import org.apache.aurora.scheduler.storage.Storage.Work;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;

/**
 * Auxiliary class to simplify testing against a mocked storage.  This allows callers to directly
 * set up call expectations on individual stores rather than writing plumbing code to deal with
 * operations and {@link StoreProvider}.
 */
public class StorageTestUtil {

  public final StoreProvider storeProvider;
  public final MutableStoreProvider mutableStoreProvider;
  public final TaskStore.Mutable taskStore;
  public final QuotaStore.Mutable quotaStore;
  public final AttributeStore.Mutable attributeStore;
  public final JobStore.Mutable jobStore;
  public final LockStore.Mutable lockStore;
  public final SchedulerStore.Mutable schedulerStore;
  public final JobUpdateStore.Mutable jobUpdateStore;
  public final NonVolatileStorage storage;

  /**
   * Creates a new storage test utility.
   *
   * @param easyMock Mocking framework to use for setting up mocks and expectations.
   */
  public StorageTestUtil(EasyMockTest easyMock) {
    this.storeProvider = easyMock.createMock(StoreProvider.class);
    this.mutableStoreProvider = easyMock.createMock(MutableStoreProvider.class);
    this.taskStore = easyMock.createMock(TaskStore.Mutable.class);
    this.quotaStore = easyMock.createMock(QuotaStore.Mutable.class);
    this.attributeStore = easyMock.createMock(AttributeStore.Mutable.class);
    this.jobStore = easyMock.createMock(JobStore.Mutable.class);
    this.lockStore = easyMock.createMock(LockStore.Mutable.class);
    this.schedulerStore = easyMock.createMock(SchedulerStore.Mutable.class);
    this.jobUpdateStore = easyMock.createMock(JobUpdateStore.Mutable.class);
    this.storage = easyMock.createMock(NonVolatileStorage.class);
  }

  public <T> IExpectationSetters<T> expectRead() {
    final Capture<Work<T, RuntimeException>> work = EasyMockTest.createCapture();
    return expect(storage.read(capture(work)))
        .andAnswer(new IAnswer<T>() {
          @Override
          public T answer() {
            return work.getValue().apply(storeProvider);
          }
        });
  }

  public <T> IExpectationSetters<T> expectWrite() {
    final Capture<MutateWork<T, RuntimeException>> work = EasyMockTest.createCapture();
    return expect(storage.write(capture(work))).andAnswer(new IAnswer<T>() {
      @Override
      public T answer() {
        return work.getValue().apply(mutableStoreProvider);
      }
    });
  }

  /**
   * Expects any number of read or write operations.
   */
  public void expectOperations() {
    expect(storeProvider.getTaskStore()).andReturn(taskStore).anyTimes();
    expect(storeProvider.getQuotaStore()).andReturn(quotaStore).anyTimes();
    expect(storeProvider.getAttributeStore()).andReturn(attributeStore).anyTimes();
    expect(storeProvider.getJobStore()).andReturn(jobStore).anyTimes();
    expect(storeProvider.getLockStore()).andReturn(lockStore).anyTimes();
    expect(storeProvider.getSchedulerStore()).andReturn(schedulerStore).anyTimes();
    expect(storeProvider.getJobUpdateStore()).andReturn(jobUpdateStore).anyTimes();
    expect(mutableStoreProvider.getTaskStore()).andReturn(taskStore).anyTimes();
    expect(mutableStoreProvider.getUnsafeTaskStore()).andReturn(taskStore).anyTimes();
    expect(mutableStoreProvider.getQuotaStore()).andReturn(quotaStore).anyTimes();
    expect(mutableStoreProvider.getAttributeStore()).andReturn(attributeStore).anyTimes();
    expect(mutableStoreProvider.getJobStore()).andReturn(jobStore).anyTimes();
    expect(mutableStoreProvider.getLockStore()).andReturn(lockStore).anyTimes();
    expect(mutableStoreProvider.getSchedulerStore()).andReturn(schedulerStore).anyTimes();
    expect(mutableStoreProvider.getJobUpdateStore()).andReturn(jobUpdateStore).anyTimes();
    expectRead().anyTimes();
    expectWrite().anyTimes();
  }

  public IExpectationSetters<?> expectTaskFetch(
      Query.Builder query,
      ImmutableSet<IScheduledTask> result) {

    return expect(taskStore.fetchTasks(query)).andReturn(result);
  }

  public IExpectationSetters<?> expectTaskFetch(Query.Builder query, IScheduledTask... result) {
    return expectTaskFetch(query, ImmutableSet.<IScheduledTask>builder().add(result).build());
  }
}
