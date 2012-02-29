package com.twitter.mesos.scheduler.storage.log;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import org.easymock.Capture;
import org.easymock.IAnswer;
import org.junit.Before;
import org.junit.Test;

import com.twitter.common.testing.EasyMockTest;
import com.twitter.common.util.testing.FakeClock;
import com.twitter.mesos.gen.Attribute;
import com.twitter.mesos.gen.HostAttributes;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.storage.JobUpdateConfiguration;
import com.twitter.mesos.gen.storage.QuotaConfiguration;
import com.twitter.mesos.gen.storage.SchedulerMetadata;
import com.twitter.mesos.gen.storage.Snapshot;
import com.twitter.mesos.gen.storage.StoredJob;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.quota.Quotas;
import com.twitter.mesos.scheduler.storage.AttributeStore;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.StoreProvider;
import com.twitter.mesos.scheduler.storage.Storage.Work;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;

/**
 * @author William Farner
 */
public class SnapshotStoreImplTest extends EasyMockTest {

  private static final long NOW = 10335463456L;

  private SnapshotStore<byte[]> binarySnapshotStore;
  private StoreProvider storeProvider;
  private TaskStore taskStore;
  private QuotaStore quotaStore;
  private AttributeStore attributeStore;
  private JobStore jobStore;
  private UpdateStore updateStore;
  private SchedulerStore schedulerStore;
  private Storage storage;
  private SnapshotStore<Snapshot> snapshotStore;

  @Before
  public void setUp() {
    FakeClock clock = new FakeClock();
    clock.setNowMillis(NOW);
    binarySnapshotStore = createMock(new Clazz<SnapshotStore<byte[]>>() {});
    storeProvider = createMock(StoreProvider.class);
    taskStore = createMock(TaskStore.class);
    quotaStore = createMock(QuotaStore.class);
    attributeStore = createMock(AttributeStore.class);
    jobStore = createMock(JobStore.class);
    updateStore = createMock(UpdateStore.class);
    schedulerStore = createMock(SchedulerStore.class);
    storage = createMock(Storage.class);
    snapshotStore = new SnapshotStoreImpl(clock, binarySnapshotStore, storage);
  }

  @Test
  public void testRestoreOldSnapshot() {
    byte[] snapshotData = "binary snapshot".getBytes();

    expectTransactions();
    attributeStore.deleteHostAttributes();
    binarySnapshotStore.applySnapshot(snapshotData);

    control.replay();

    snapshotStore.applySnapshot(new Snapshot()
        .setDataDEPRECATED(snapshotData));
  }

  @Test
  public void testCreateAndRestoreNewSnapshot() {
    ImmutableSet<ScheduledTask> tasks =
        ImmutableSet.of(new ScheduledTask().setStatus(ScheduleStatus.PENDING));
    Set<QuotaConfiguration> quotas =
        ImmutableSet.of(new QuotaConfiguration("steve", Quotas.NO_QUOTA));
    HostAttributes attribute = new HostAttributes("host",
        ImmutableSet.of(new Attribute("attr", ImmutableSet.of("value"))));
    StoredJob job = new StoredJob("jobManager", new JobConfiguration().setName("name"));
    JobUpdateConfiguration update = new JobUpdateConfiguration("role", "job", "token",
        ImmutableSet.<TaskUpdateConfiguration>of());
    String frameworkId = "framework_id";

    expectTransactions();
    expect(taskStore.fetchTasks(Query.GET_ALL)).andReturn(tasks);
    expect(quotaStore.fetchQuotaRoles()).andReturn(ImmutableSet.of("steve"));
    expect(quotaStore.fetchQuota("steve")).andReturn(Optional.of(Quotas.NO_QUOTA));
    expect(attributeStore.getHostAttributes()).andReturn(ImmutableSet.of(attribute));
    expect(jobStore.fetchManagerIds()).andReturn(ImmutableSet.of("jobManager"));
    expect(jobStore.fetchJobs("jobManager")).andReturn(ImmutableSet.of(job.getJobConfiguration()));
    expect(updateStore.fetchUpdatingRoles()).andReturn(ImmutableSet.of("role"));
    expect(updateStore.fetchUpdateConfigs("role")).andReturn(ImmutableSet.of(update));
    expect(schedulerStore.fetchFrameworkId()).andReturn(frameworkId);

    expectDataWipe();
    taskStore.saveTasks(tasks);
    quotaStore.saveQuota("steve", Quotas.NO_QUOTA);
    attributeStore.saveHostAttributes(attribute);
    jobStore.saveAcceptedJob(job.getJobManagerId(), job.getJobConfiguration());
    updateStore.saveJobUpdateConfig(update);
    schedulerStore.saveFrameworkId(frameworkId);

    control.replay();

    Snapshot expected = new Snapshot()
        .setTimestamp(NOW)
        .setTasks(tasks)
        .setQuotaConfigurations(quotas)
        .setHostAttributes(ImmutableSet.of(attribute))
        .setJobs(ImmutableSet.of(job))
        .setUpdateConfigurations(ImmutableSet.of(update))
        .setSchedulerMetadata(new SchedulerMetadata(frameworkId));
    assertEquals(expected, snapshotStore.createSnapshot());

    snapshotStore.applySnapshot(expected);
  }

  @Test(expected = IllegalStateException.class)
  public void testRestoreMixedSnapshot() {
    expectTransactions();

    control.replay();

    snapshotStore.applySnapshot(new Snapshot()
        .setDataDEPRECATED("binary snapshot".getBytes())
        .setTasks(ImmutableSet.<ScheduledTask>of()));
  }

  private void expectDataWipe() {
    taskStore.removeTasks(Query.GET_ALL);
    quotaStore.deleteQuotas();
    attributeStore.deleteHostAttributes();
    jobStore.deleteJobs();
    updateStore.deleteShardUpdateConfigs();
  }

  private <T> void expectTransactions() {
    expect(storeProvider.getTaskStore()).andReturn(taskStore).anyTimes();
    expect(storeProvider.getQuotaStore()).andReturn(quotaStore).anyTimes();
    expect(storeProvider.getAttributeStore()).andReturn(attributeStore).anyTimes();
    expect(storeProvider.getJobStore()).andReturn(jobStore).anyTimes();
    expect(storeProvider.getUpdateStore()).andReturn(updateStore).anyTimes();
    expect(storeProvider.getSchedulerStore()).andReturn(schedulerStore).anyTimes();

    final Capture<Work<T, RuntimeException>> work = createCapture();
    expect(storage.doInTransaction(capture(work))).andAnswer(new IAnswer<T>() {
      @Override
      public T answer() {
        return work.getValue().apply(storeProvider);
      }
    }).anyTimes();
  }
}
