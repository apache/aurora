package com.twitter.mesos.scheduler.storage.testing;

import org.easymock.Capture;
import org.easymock.IAnswer;
import org.easymock.IExpectationSetters;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;

/**
 * Auxiliary class to simplify testing against a mocked storage.  This allows callers to directly
 * set up call expectations on individual stores rather than writing plumbing code to deal with
 * transactions and {@link StoreProvider}.
 *
 * @author William Farner
 */
public class StorageTestUtil {

  public final StoreProvider storeProvider;
  public final TaskStore taskStore;
  public final QuotaStore quotaStore;
  public final AttributeStore attributeStore;
  public final JobStore jobStore;
  public final UpdateStore updateStore;
  public final SchedulerStore schedulerStore;
  public final Storage storage;

  /**
   * Creates a new storage test utility.
   *
   * @param easyMock Mocking framework to use for setting up mocks and expectations.
   */
  public StorageTestUtil(EasyMockTest easyMock) {
    this.storeProvider = easyMock.createMock(StoreProvider.class);
    this.taskStore = easyMock.createMock(TaskStore.class);
    this.quotaStore = easyMock.createMock(QuotaStore.class);
    this.attributeStore = easyMock.createMock(AttributeStore.class);
    this.jobStore = easyMock.createMock(JobStore.class);
    this.updateStore = easyMock.createMock(UpdateStore.class);
    this.schedulerStore = easyMock.createMock(SchedulerStore.class);
    this.storage = easyMock.createMock(Storage.class);
  }

  /**
   * Sets up an expectation for a single transaction, which may be chained to produce a result
   * such as a return value or exception.
   *
   * @param <T> Return value type.
   * @return A call expectation setter.
   */
  public <T> IExpectationSetters<T> expectTransaction() {
    expect(storeProvider.getTaskStore()).andReturn(taskStore).anyTimes();
    expect(storeProvider.getQuotaStore()).andReturn(quotaStore).anyTimes();
    expect(storeProvider.getAttributeStore()).andReturn(attributeStore).anyTimes();
    expect(storeProvider.getJobStore()).andReturn(jobStore).anyTimes();
    expect(storeProvider.getUpdateStore()).andReturn(updateStore).anyTimes();
    expect(storeProvider.getSchedulerStore()).andReturn(schedulerStore).anyTimes();

    final Capture<Work<T, RuntimeException>> work = EasyMockTest.createCapture();
    return expect(storage.doInTransaction(capture(work))).andAnswer(new IAnswer<T>() {
      @Override public T answer() {
        return work.getValue().apply(storeProvider);
      }
    });
  }

  /**
   * Expects any number of transactions.
   */
  public void expectTransactions() {
    expectTransaction().anyTimes();
  }
}
