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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateConfiguration;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
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
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class DBJobUpdateStoreTest {

  private static final IJobKey JOB = JobKeys.from("testRole", "testEnv", "job");
  private static final long CREATED_MS = 111L;
  private static final IJobUpdateEvent FIRST_EVENT =
      makeJobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, CREATED_MS);

  private Storage storage;

  @Before
  public void setUp() throws Exception {
    storage = DbUtil.createStorage();
  }

  @After
  public void tearDown() throws Exception {
    truncateUpdates();
  }

  @Test
  public void testSaveJobUpdates() {
    String updateId1 = "u1";
    String updateId2 = "u2";

    IJobUpdate update1 = makeJobUpdate(JOB, updateId1);
    IJobUpdate update2 = makeJobUpdate(JOB, updateId2);

    saveUpdate(update1);
    assertEquals(populateExpected(update1), getUpdateDetails(updateId1).get().getUpdate());

    saveUpdate(update2);
    assertEquals(populateExpected(update1), getUpdateDetails(updateId1).get().getUpdate());
  }

  @Test
  public void testSaveJobUpdateEmptyInstanceOverrides() {
    String updateId = "u1";

    IJobUpdate update = makeJobUpdate(JOB, updateId);
    JobUpdate builder = update.newBuilder();
    builder.getConfiguration().getSettings().setUpdateOnlyTheseInstances(ImmutableSet.<Range>of());

    IJobUpdate expected = IJobUpdate.build(builder);

    // Save with empty overrides.
    saveUpdate(expected);
    assertEquals(populateExpected(expected), getUpdateDetails(updateId).get().getUpdate());
  }

  @Test
  public void testSaveJobUpdateNullInstanceOverrides() {
    String updateId = "u1";

    IJobUpdate update = makeJobUpdate(JOB, updateId);
    JobUpdate builder = update.newBuilder();
    builder.getConfiguration().getSettings().setUpdateOnlyTheseInstances(ImmutableSet.<Range>of());

    IJobUpdate expected = IJobUpdate.build(builder);

    // Save with null overrides.
    builder.getConfiguration().getSettings().setUpdateOnlyTheseInstances(null);
    saveUpdate(IJobUpdate.build(builder));
    assertEquals(populateExpected(expected), getUpdateDetails(updateId).get().getUpdate());
  }

  @Test(expected = StorageException.class)
  public void testSaveJobUpdateTwiceThrows() {
    String updateId = "u1";
    IJobUpdate update = makeJobUpdate(JOB, updateId);

    saveUpdate(update);
    saveUpdate(update);
  }

  @Test
  public void testSaveJobEvents() {
    String updateId = "u3";
    IJobUpdate update = makeJobUpdate(JOB, updateId);
    IJobUpdateEvent event1 = makeJobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 124L);
    IJobUpdateEvent event2 = makeJobUpdateEvent(JobUpdateStatus.ROLL_FORWARD_PAUSED, 125L);

    saveUpdate(update);
    assertEquals(populateExpected(update), getUpdateDetails(updateId).get().getUpdate());
    assertEquals(ImmutableList.of(FIRST_EVENT), getUpdateDetails(updateId).get().getUpdateEvents());

    saveJobEvent(event1, updateId);
    assertEquals(
        populateExpected(update, JobUpdateStatus.ROLLING_FORWARD, CREATED_MS, 124L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(1));

    saveJobEvent(event2, updateId);
    assertEquals(
        populateExpected(update, JobUpdateStatus.ROLL_FORWARD_PAUSED, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(1));
    assertEquals(event2, getUpdateDetails(updateId).get().getUpdateEvents().get(2));
  }

  @Test
  public void testSaveInstanceEvents() {
    String updateId = "u3";
    IJobUpdate update = makeJobUpdate(JOB, updateId);
    IJobInstanceUpdateEvent event1 = makeJobInstanceEvent(0, 125L, JobUpdateAction.ADD_INSTANCE);
    IJobInstanceUpdateEvent event2 = makeJobInstanceEvent(1, 126L, JobUpdateAction.ADD_INSTANCE);

    saveUpdate(update);
    assertEquals(populateExpected(update), getUpdateDetails(updateId).get().getUpdate());
    assertEquals(0, getUpdateDetails(updateId).get().getInstanceEvents().size());

    saveJobInstanceEvent(event1, updateId);
    assertEquals(
        populateExpected(update, JobUpdateStatus.ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(
        event1,
        Iterables.getOnlyElement(getUpdateDetails(updateId).get().getInstanceEvents()));

    saveJobInstanceEvent(event2, updateId);
    assertEquals(
        populateExpected(update, JobUpdateStatus.ROLLING_FORWARD, CREATED_MS, 126L),
        getUpdateDetails(updateId).get().getUpdate());
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
  public void testSaveJobUpdateStateIgnored() {
    String updateId = "u1";

    IJobUpdate update = populateExpected(
        makeJobUpdate(JOB, updateId),
        JobUpdateStatus.ABORTED,
        567L,
        567L);

    saveUpdate(update);

    // Assert state fields were ignored.
    assertEquals(populateExpected(update), getUpdateDetails(updateId).get().getUpdate());
  }

  @Test
  public void testSaveJobUpdateWithoutEventFailsSelect() {
    final String updateId = "u3";
    storage.write(new MutateWork.Quiet<Void>() {
      @Override
      public Void apply(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(makeJobUpdate(JOB, updateId));
        return null;
      }
    });
    assertEquals(Optional.<IJobUpdateDetails>absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testMultipleJobDetails() {
    String updateId1 = "u1";
    String updateId2 = "u2";
    IJobUpdateDetails details1 = makeJobDetails(makeJobUpdate(JOB, updateId1));
    IJobUpdateDetails details2 = makeJobDetails(makeJobUpdate(JOB, updateId2));

    saveUpdate(details1.getUpdate());
    saveUpdate(details2.getUpdate());

    details1 = updateJobDetails(populateExpected(details1.getUpdate()), FIRST_EVENT);
    details2 = updateJobDetails(populateExpected(details2.getUpdate()), FIRST_EVENT);
    assertEquals(details1, getUpdateDetails(updateId1).get());
    assertEquals(details2, getUpdateDetails(updateId2).get());

    IJobUpdateEvent jEvent11 = makeJobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 456L);
    IJobUpdateEvent jEvent12 = makeJobUpdateEvent(JobUpdateStatus.ERROR, 457L);
    IJobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, JobUpdateAction.ADD_INSTANCE);
    IJobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 452L, JobUpdateAction.ADD_INSTANCE);

    IJobUpdateEvent jEvent21 = makeJobUpdateEvent(JobUpdateStatus.ROLL_FORWARD_PAUSED, 567L);
    IJobUpdateEvent jEvent22 = makeJobUpdateEvent(JobUpdateStatus.ABORTED, 568L);
    IJobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, JobUpdateAction.ADD_INSTANCE);
    IJobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(4, 562L, JobUpdateAction.ADD_INSTANCE);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);
    saveJobInstanceEvent(iEvent21, updateId2);
    saveJobInstanceEvent(iEvent22, updateId2);

    details1 = updateJobDetails(
        populateExpected(details1.getUpdate(), JobUpdateStatus.ERROR, CREATED_MS, 457L),
        ImmutableList.of(FIRST_EVENT, jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    details2 = updateJobDetails(
        populateExpected(details2.getUpdate(), JobUpdateStatus.ABORTED, CREATED_MS, 568L),
        ImmutableList.of(FIRST_EVENT, jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    assertEquals(details1, getUpdateDetails(updateId1).get());
    assertEquals(details2, getUpdateDetails(updateId2).get());

    assertEquals(ImmutableList.of(details1, details2), getAllUpdateDetails());
  }

  @Test
  public void testTruncateJobUpdates() {
    String updateId = "u5";
    IJobUpdate update = makeJobUpdate(JOB, updateId);
    IJobUpdateEvent updateEvent = IJobUpdateEvent.build(
        new JobUpdateEvent(JobUpdateStatus.ROLLING_FORWARD, 123L));
    IJobInstanceUpdateEvent instanceEvent = IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(0, 125L, JobUpdateAction.ADD_INSTANCE));

    saveUpdate(update);
    saveJobEvent(updateEvent, updateId);
    saveJobInstanceEvent(instanceEvent, updateId);
    assertEquals(
        populateExpected(update, JobUpdateStatus.ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(2, getUpdateDetails(updateId).get().getUpdateEvents().size());
    assertEquals(1, getUpdateDetails(updateId).get().getInstanceEvents().size());

    truncateUpdates();
    assertEquals(Optional.<IJobUpdateDetails>absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testGetSummaries() {
    IJobKey job2 = JobKeys.from("role", "env", "name");
    IJobUpdateSummary s1 = saveSummary(JOB, "u1", 1230L, JobUpdateStatus.ROLLED_BACK, "user");
    IJobUpdateSummary s2 = saveSummary(JOB, "u2", 1231L, JobUpdateStatus.ABORTED, "user");
    IJobUpdateSummary s3 = saveSummary(JOB, "u3", 1239L, JobUpdateStatus.ERROR, "user2");
    IJobUpdateSummary s4 = saveSummary(JOB, "u4", 1234L, JobUpdateStatus.ROLL_BACK_PAUSED, "user3");
    IJobUpdateSummary s5 = saveSummary(job2, "u5", 1235L, JobUpdateStatus.ROLLING_FORWARD, "user4");

    // Test empty query returns all.
    assertEquals(ImmutableList.of(s1, s2, s4, s5, s3), getSummaries(new JobUpdateQuery()));

    // Test query by updateId.
    assertEquals(ImmutableList.of(s1), getSummaries(new JobUpdateQuery().setUpdateId("u1")));

    // Test query by role.
    assertEquals(
        ImmutableList.of(s1, s2, s4, s3),
        getSummaries(new JobUpdateQuery().setRole(JOB.getRole())));

    // Test query by job key.
    assertEquals(
        ImmutableList.of(s5),
        getSummaries(new JobUpdateQuery().setJobKey(job2.newBuilder())));

    // Test query by user.
    assertEquals(ImmutableList.of(s1, s2), getSummaries(new JobUpdateQuery().setUser("user")));

    // Test query by one status.
    assertEquals(ImmutableList.of(s3), getSummaries(new JobUpdateQuery().setUpdateStatuses(
        ImmutableSet.of(JobUpdateStatus.ERROR))));

    // Test query by multiple statuses.
    assertEquals(ImmutableList.of(s1, s2, s3), getSummaries(new JobUpdateQuery().setUpdateStatuses(
        ImmutableSet.of(
            JobUpdateStatus.ERROR,
            JobUpdateStatus.ABORTED,
            JobUpdateStatus.ROLLED_BACK))));

    // Test query by empty statuses.
    assertEquals(
        ImmutableList.of(s1, s2, s4, s5, s3),
        getSummaries(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.<JobUpdateStatus>of())));

    // Test paging.
    assertEquals(
        ImmutableList.of(s1, s2),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(0)));
    assertEquals(
        ImmutableList.of(s4, s5),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(2)));
    assertEquals(
        ImmutableList.of(s3),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(4)));

    // Test no match.
    assertEquals(
        ImmutableList.<IJobUpdateSummary>of(),
        getSummaries(new JobUpdateQuery().setRole("no_match")));
  }

  private Optional<IJobUpdateDetails> getUpdateDetails(final String updateId) {
    return storage.consistentRead(new Quiet<Optional<IJobUpdateDetails>>() {
      @Override
      public Optional<IJobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(updateId);
      }
    });
  }

  private List<IJobUpdateDetails> getAllUpdateDetails() {
    return storage.consistentRead(new Quiet<List<IJobUpdateDetails>>() {
      @Override
      public List<IJobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchAllJobUpdateDetails();
      }
    });
  }

  private List<IJobUpdateSummary> getSummaries(final JobUpdateQuery query) {
    return storage.consistentRead(new Quiet<List<IJobUpdateSummary>>() {
      @Override
      public List<IJobUpdateSummary> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateSummaries(
            IJobUpdateQuery.build(query));
      }
    });
  }

  private void saveUpdate(final IJobUpdate update) {
    storage.write(new MutateWork.Quiet<Void>() {
      @Override
      public Void apply(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(update);
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(
            FIRST_EVENT,
            update.getSummary().getUpdateId());
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

  private IJobUpdate populateExpected(IJobUpdate update) {
    return populateExpected(update, JobUpdateStatus.ROLLING_FORWARD, CREATED_MS, CREATED_MS);
  }

  private IJobUpdate populateExpected(
      IJobUpdate update,
      JobUpdateStatus status,
      long createdMs,
      long lastMs) {

    JobUpdateState state = new JobUpdateState()
        .setCreatedTimestampMs(createdMs)
        .setLastModifiedTimestampMs(lastMs)
        .setStatus(status);
    JobUpdate builder = update.newBuilder();
    builder.getSummary().setState(state);
    return IJobUpdate.build(builder);
  }

  private static IJobUpdateEvent makeJobUpdateEvent(JobUpdateStatus status, long timestampMs) {
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
        ImmutableList.<IJobUpdateEvent>of(FIRST_EVENT),
        ImmutableList.<IJobInstanceUpdateEvent>of());
  }

  private IJobUpdateDetails updateJobDetails(IJobUpdate update, IJobUpdateEvent event) {
    return updateJobDetails(
        update,
        ImmutableList.of(event),
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

  private IJobUpdateSummary makeSummary(IJobKey jobKey, String updateId, String user) {
    return IJobUpdateSummary.build(new JobUpdateSummary()
        .setUpdateId(updateId)
        .setJobKey(jobKey.newBuilder())
        .setUser(user));
  }

  private IJobUpdateSummary saveSummary(
      IJobKey jobKey,
      String updateId,
      Long modifiedTimestampMs,
      JobUpdateStatus status,
      String user) {

    IJobUpdateSummary summary = IJobUpdateSummary.build(new JobUpdateSummary()
        .setUpdateId(updateId)
        .setJobKey(jobKey.newBuilder())
        .setUser(user));

    IJobUpdate update = makeJobUpdate(summary);
    saveUpdate(update);
    saveJobEvent(makeJobUpdateEvent(status, modifiedTimestampMs), updateId);
    return populateExpected(update, status, CREATED_MS, modifiedTimestampMs).getSummary();
  }

  private IJobUpdate makeJobUpdate(IJobUpdateSummary summary) {
    return IJobUpdate.build(makeJobUpdate().newBuilder().setSummary(summary.newBuilder()));
  }

  private IJobUpdate makeJobUpdate(IJobKey jobKey, String updateId) {
    return IJobUpdate.build(makeJobUpdate().newBuilder()
        .setSummary(makeSummary(jobKey, updateId, "user").newBuilder()));
  }

  private IJobUpdate makeJobUpdate() {
    return IJobUpdate.build(new JobUpdate()
        .setConfiguration(new JobUpdateConfiguration()
            .setNewTaskConfig(makeTaskConfig())
            .setInstanceCount(8)
            .setOldTaskConfigs(ImmutableSet.of(
                new InstanceTaskConfig()
                    .setInstances(ImmutableSet.of(new Range(0, 1), new Range(2, 3)))
                    .setTask(makeTaskConfig()),
                new InstanceTaskConfig()
                    .setInstances(ImmutableSet.of(new Range(4, 5), new Range(6, 7)))
                    .setTask(makeTaskConfig())))
            .setSettings(new JobUpdateSettings()
                .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 0), new Range(3, 5))))));
  }

  private TaskConfig makeTaskConfig() {
    return new TaskConfig()
        .setJobName(JOB.getName())
        .setEnvironment(JOB.getEnvironment())
        .setOwner(new Identity(JOB.getRole(), "user"))
        .setIsService(true);
  }
}
