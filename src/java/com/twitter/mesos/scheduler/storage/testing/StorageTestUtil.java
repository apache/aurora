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
 * Auxiliary class to simplify testing against a mocked storage.
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

  public void expectTransactions() {
    expectTransaction().anyTimes();
  }
}
