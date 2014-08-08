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

import java.util.List;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.twitter.common.inject.Bindings;

import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateConfiguration;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.storage.Storage.MutableStoreProvider;
import org.apache.aurora.scheduler.storage.Storage.MutateWork;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.Storage.Work.Quiet;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DBJobUpdateStoreTest {

  private static final IJobKey JOB = JobKeys.from("testRole", "testEnv", "job");

  private DbStorage storage;

  @Before
  public void setUp() throws Exception {
    Injector injector = Guice.createInjector(DbModule.testModule(Bindings.KeyFactory.PLAIN));
    storage = injector.getInstance(DbStorage.class);
    storage.prepare();
  }

  @After
  public void tearDown() throws Exception {
    truncateUpdates();
  }

  @Test
  public void testSaveJobUpdates() {
    String updateId1 = "u1";
    String updateId2 = "u2";
    IJobUpdate update1 = makeJobUpdate(JOB, updateId1, JobUpdateStatus.INIT);
    IJobUpdate update2 = makeJobUpdate(JOB, updateId2, JobUpdateStatus.INIT);

    saveUpdate(update1);
    assertEquals(update1, getUpdateDetails(updateId1).get().getUpdate());

    saveUpdate(update2);
    assertEquals(update2, getUpdateDetails(updateId2).get().getUpdate());
  }

  @Test
  public void testSaveJobUpdateTwice() {
    String updateId = "u1";
    IJobUpdate update = makeJobUpdate(JOB, updateId, JobUpdateStatus.INIT);

    saveUpdate(update);
    assertEquals(update, getUpdateDetails(updateId).get().getUpdate());

    JobUpdate builder = update.newBuilder();
    builder.getSummary().setStatus(JobUpdateStatus.ABORTED);
    update = IJobUpdate.build(builder);
    saveUpdate(update);
    assertEquals(update, getUpdateDetails(updateId).get().getUpdate());
  }

  @Test
  public void testSaveJobEvents() {
    String updateId = "u3";
    IJobUpdate update = makeJobUpdate(JOB, updateId, JobUpdateStatus.ROLLING_FORWARD);
    IJobUpdateEvent event1 = makeJobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L);
    IJobUpdateEvent event2 = makeJobUpdateEvent(JobUpdateStatus.ROLL_FORWARD_PAUSED, 124L);

    saveUpdate(update);
    assertEquals(update, getUpdateDetails(updateId).get().getUpdate());
    assertEquals(0, getUpdateDetails(updateId).get().getUpdateEvents().size());

    saveJobEvent(event2, updateId);
    assertEquals(event2, getUpdateDetails(updateId).get().getUpdateEvents().get(0));

    saveJobEvent(event1, updateId);
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(0));
    assertEquals(event2, getUpdateDetails(updateId).get().getUpdateEvents().get(1));
  }

  @Test
  public void testSaveInstanceEvents() {
    String updateId = "u3";
    IJobUpdate update = makeJobUpdate(JOB, updateId, JobUpdateStatus.ROLLING_FORWARD);
    IJobInstanceUpdateEvent event1 = makeJobInstanceEvent(0, 125L, JobUpdateAction.ADD_INSTANCE);
    IJobInstanceUpdateEvent event2 = makeJobInstanceEvent(1, 126L, JobUpdateAction.ADD_INSTANCE);

    saveUpdate(update);
    assertEquals(update, getUpdateDetails(updateId).get().getUpdate());
    assertEquals(0, getUpdateDetails(updateId).get().getInstanceEvents().size());

    saveJobInstanceEvent(event2, updateId);
    assertEquals(1, getUpdateDetails(updateId).get().getInstanceEvents().size());

    saveJobInstanceEvent(event1, updateId);
    assertEquals(2, getUpdateDetails(updateId).get().getInstanceEvents().size());

    // Test event order.
    assertEquals(event1, getUpdateDetails(updateId).get().getInstanceEvents().get(0));
    assertEquals(event2, getUpdateDetails(updateId).get().getInstanceEvents().get(1));
  }

  @Test(expected = StorageException.class)
  public void testSaveJobEventWithoutUpdateFails() {
    saveJobEvent(makeJobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L), "u2");
  }

  @Test(expected = StorageException.class)
  public void testSaveInstanceEventWithoutUpdateFails() {
    saveJobInstanceEvent(makeJobInstanceEvent(0, 125L, JobUpdateAction.ADD_INSTANCE), "u1");
  }

  @Test
  public void testMultipleJobDetails() {
    String updateId1 = "u1";
    String updateId2 = "u2";
    IJobUpdateDetails details1 = makeJobDetails(
        makeJobUpdate(JOB, updateId1, JobUpdateStatus.ABORTED));

    IJobUpdateDetails details2 = makeJobDetails(
        makeJobUpdate(JOB, updateId2, JobUpdateStatus.ERROR));

    saveUpdate(details1.getUpdate());
    saveUpdate(details2.getUpdate());
    assertEquals(details1, getUpdateDetails(updateId1).get());
    assertEquals(details2, getUpdateDetails(updateId2).get());

    IJobUpdateEvent jEvent11 = makeJobUpdateEvent(JobUpdateStatus.ROLL_BACK_PAUSED, 456L);
    IJobUpdateEvent jEvent12 = makeJobUpdateEvent(JobUpdateStatus.ERROR, 457L);
    IJobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, JobUpdateAction.ADD_INSTANCE);
    IJobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 452L, JobUpdateAction.ADD_INSTANCE);

    IJobUpdateEvent jEvent21 = makeJobUpdateEvent(JobUpdateStatus.ROLL_FORWARD_PAUSED, 567L);
    IJobUpdateEvent jEvent22 = makeJobUpdateEvent(JobUpdateStatus.ABORTED, 568L);
    IJobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, JobUpdateAction.ADD_INSTANCE);
    IJobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(4, 562L, JobUpdateAction.ADD_INSTANCE);

    details1 = updateJobDetails(
        details1.getUpdate(),
        ImmutableList.of(jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    details2 = updateJobDetails(
        details2.getUpdate(),
        ImmutableList.of(jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);
    saveJobInstanceEvent(iEvent21, updateId2);
    saveJobInstanceEvent(iEvent22, updateId2);

    assertEquals(details1, getUpdateDetails(updateId1).get());
    assertEquals(details2, getUpdateDetails(updateId2).get());
  }

  @Test
  public void testTruncateJobUpdates() {
    String updateId = "u5";
    IJobUpdate update = makeJobUpdate(JOB, updateId, JobUpdateStatus.INIT);
    IJobUpdateEvent updateEvent = IJobUpdateEvent.build(
        new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L));
    IJobInstanceUpdateEvent instanceEvent = IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(0, 125L, JobUpdateAction.ADD_INSTANCE));

    saveUpdate(update);
    saveJobEvent(updateEvent, updateId);
    saveJobInstanceEvent(instanceEvent, updateId);
    assertEquals(update, getUpdateDetails(updateId).get().getUpdate());
    assertEquals(1, getUpdateDetails(updateId).get().getUpdateEvents().size());
    assertEquals(1, getUpdateDetails(updateId).get().getInstanceEvents().size());

    truncateUpdates();
    assertEquals(Optional.<IJobUpdateDetails>absent(), getUpdateDetails(updateId));
  }

  private Optional<IJobUpdateDetails> getUpdateDetails(final String updateId) {
    return storage.consistentRead(new Quiet<Optional<IJobUpdateDetails>>() {
      @Override
      public Optional<IJobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(updateId);
      }
    });
  }

  private void saveUpdate(final IJobUpdate update) {
    storage.write(new MutateWork.Quiet<Void>() {
      @Override
      public Void apply(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(update);
        return null;
      }
    });
  }

  private void saveJobEvent(final IJobUpdateEvent event, final String updateId) {
    storage.write(new MutateWork.Quiet<Void>() {
      @Override
      public Void apply(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(event, updateId);
        return null;
      }
    });
  }

  private void saveJobInstanceEvent(final IJobInstanceUpdateEvent event, final String updateId) {
    storage.write(new MutateWork.Quiet<Void>() {
      @Override
      public Void apply(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(event, updateId);
        return null;
      }
    });
  }

  private void truncateUpdates() {
    storage.write(new MutateWork.Quiet<Void>() {
      @Override
      public Void apply(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
        return null;
      }
    });
  }

  private IJobUpdateEvent makeJobUpdateEvent(JobUpdateStatus status, long timestampMs) {
    return IJobUpdateEvent.build(
        new JobUpdateEvent(status, timestampMs));
  }

  private IJobInstanceUpdateEvent makeJobInstanceEvent(
      int instanceId,
      long timestampMs,
      JobUpdateAction action) {
    return IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(instanceId, timestampMs, action));
  }

  private IJobUpdateDetails makeJobDetails(IJobUpdate update) {
    return updateJobDetails(
        update,
        ImmutableList.<IJobUpdateEvent>of(),
        ImmutableList.<IJobInstanceUpdateEvent>of());
  }

  private IJobUpdateDetails updateJobDetails(
      IJobUpdate update,
      List<IJobUpdateEvent> jobEvents,
      List<IJobInstanceUpdateEvent> instanceEvents) {

    return IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdate(update.newBuilder())
        .setUpdateEvents(IJobUpdateEvent.toBuildersList(jobEvents))
        .setInstanceEvents(IJobInstanceUpdateEvent.toBuildersList(instanceEvents)));
  }

  private IJobUpdate makeJobUpdate(IJobKey jobKey, String updateId, JobUpdateStatus status) {
    return IJobUpdate.build(new JobUpdate()
        .setSummary(new JobUpdateSummary()
            .setUpdateId(updateId)
            .setJobKey(jobKey.newBuilder())
            .setUser("user")
            .setStatus(status)
            .setCreatedTimestampMs(1223L)
            .setLastModifiedTimestampMs(1224L))
        .setConfiguration(new JobUpdateConfiguration()
            .setSettings(new JobUpdateSettings())));
  }
}
