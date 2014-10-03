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
import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import org.apache.aurora.gen.Identity;
import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Lock;
import org.apache.aurora.gen.LockKey;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.storage.StoredJobUpdateDetails;
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
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.entities.ILock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLBACK_FAILED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATING;
import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.FAILED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_BACK_PAUSED;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.junit.Assert.assertEquals;

public class DBJobUpdateStoreTest {

  private static final IJobKey JOB = JobKeys.from("testRole", "testEnv", "job");
  private static final long CREATED_MS = 111L;
  private static final IJobUpdateEvent FIRST_EVENT =
      makeJobUpdateEvent(ROLLING_FORWARD, CREATED_MS);

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

    IJobUpdate update1 = makeJobUpdate(JobKeys.from("role", "env", "name1"), updateId1);
    IJobUpdate update2 = makeJobUpdate(JobKeys.from("role", "env", "name2"), updateId2);

    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId1));
    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId2));

    saveUpdate(update1, Optional.of("lock1"));
    assertUpdate(update1);

    saveUpdate(update2, Optional.<String>absent());
    assertUpdate(update2);
  }

  @Test
  public void testSaveNullInitialState() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().unsetInitialState();

    // Save with null initial state instances.
    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));

    builder.getInstructions().setInitialState(ImmutableSet.<InstanceTaskConfig>of());
    assertUpdate(IJobUpdate.build(builder));
  }

  @Test
  public void testSaveNullDesiredState() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().unsetDesiredState();

    // Save with null desired state instances.
    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));

    assertUpdate(IJobUpdate.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveBothInitialAndDesiredMissingThrows() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().unsetInitialState();
    builder.getInstructions().unsetDesiredState();

    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullInitialStateTaskThrows() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().getInitialState().add(
        new InstanceTaskConfig(null, ImmutableSet.<Range>of()));

    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyInitialStateRangesThrows() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().getInitialState().add(
        new InstanceTaskConfig(new TaskConfig(), ImmutableSet.<Range>of()));

    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullDesiredStateTaskThrows() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().getDesiredState().setTask(null);

    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyDesiredStateRangesThrows() {
    JobUpdate builder = makeJobUpdate(JOB, "u1").newBuilder();
    builder.getInstructions().getDesiredState().setInstances(ImmutableSet.<Range>of());

    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));
  }

  @Test
  public void testSaveJobUpdateEmptyInstanceOverrides() {
    String updateId = "u1";

    IJobUpdate update = makeJobUpdate(JOB, updateId);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(ImmutableSet.<Range>of());

    IJobUpdate expected = IJobUpdate.build(builder);

    // Save with empty overrides.
    saveUpdate(expected, Optional.of("lock"));
    assertUpdate(expected);
  }

  @Test
  public void testSaveJobUpdateNullInstanceOverrides() {
    String updateId = "u1";

    IJobUpdate update = makeJobUpdate(JOB, updateId);
    JobUpdate builder = update.newBuilder();
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(ImmutableSet.<Range>of());

    IJobUpdate expected = IJobUpdate.build(builder);

    // Save with null overrides.
    builder.getInstructions().getSettings().setUpdateOnlyTheseInstances(null);
    saveUpdate(IJobUpdate.build(builder), Optional.of("lock"));
    assertUpdate(expected);
  }

  @Test(expected = StorageException.class)
  public void testSaveJobUpdateTwiceThrows() {
    String updateId = "u1";
    IJobUpdate update = makeJobUpdate(JOB, updateId);

    saveUpdate(update, Optional.of("lock1"));
    saveUpdate(update, Optional.of("lock2"));
  }

  @Test
  public void testSaveJobEvents() {
    String updateId = "u3";
    String user = "test";
    IJobUpdate update = makeJobUpdate(JOB, updateId);
    IJobUpdateEvent event1 = makeJobUpdateEvent(ROLLING_FORWARD, 124L, user);
    IJobUpdateEvent event2 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 125L, user);

    saveUpdate(update, Optional.of("lock1"));
    assertUpdate(update);
    assertEquals(ImmutableList.of(FIRST_EVENT), getUpdateDetails(updateId).get().getUpdateEvents());

    saveJobEvent(event1, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 124L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(1));

    saveJobEvent(event2, updateId);
    assertEquals(
        populateExpected(update, ROLL_FORWARD_PAUSED, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getUpdateEvents().get(1));
    assertEquals(event2, getUpdateDetails(updateId).get().getUpdateEvents().get(2));
  }

  @Test
  public void testSaveInstanceEvents() {
    String updateId = "u3";
    IJobUpdate update = makeJobUpdate(JOB, updateId);
    IJobInstanceUpdateEvent event1 = makeJobInstanceEvent(0, 125L, INSTANCE_UPDATED);
    IJobInstanceUpdateEvent event2 = makeJobInstanceEvent(1, 126L, INSTANCE_ROLLING_BACK);

    saveUpdate(update, Optional.of("lock"));
    assertUpdate(update);
    assertEquals(0, getUpdateDetails(updateId).get().getInstanceEvents().size());

    saveJobInstanceEvent(event1, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(
        event1,
        Iterables.getOnlyElement(getUpdateDetails(updateId).get().getInstanceEvents()));

    saveJobInstanceEvent(event2, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 126L),
        getUpdateDetails(updateId).get().getUpdate());
    assertEquals(event1, getUpdateDetails(updateId).get().getInstanceEvents().get(0));
    assertEquals(event2, getUpdateDetails(updateId).get().getInstanceEvents().get(1));
  }

  @Test(expected = StorageException.class)
  public void testSaveJobEventWithoutUpdateFails() {
    saveJobEvent(makeJobUpdateEvent(ROLLING_FORWARD, 123L), "u2");
  }

  @Test(expected = StorageException.class)
  public void testSaveInstanceEventWithoutUpdateFails() {
    saveJobInstanceEvent(makeJobInstanceEvent(0, 125L, INSTANCE_UPDATED), "u1");
  }

  @Test
  public void testSaveJobUpdateStateIgnored() {
    String updateId = "u1";
    IJobUpdate update = populateExpected(makeJobUpdate(JOB, updateId), ABORTED, 567L, 567L);
    saveUpdate(update, Optional.of("lock1"));

    // Assert state fields were ignored.
    assertUpdate(update);
  }

  @Test
  public void testSaveJobUpdateWithoutEventFailsSelect() {
    final String updateId = "u3";
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        IJobUpdate update = makeJobUpdate(JOB, updateId);
        storeProvider.getLockStore().saveLock(makeLock(update.getSummary().getJobKey(), "lock1"));
        storeProvider.getJobUpdateStore().saveJobUpdate(update, Optional.of("lock1"));
      }
    });
    assertEquals(Optional.<IJobUpdateDetails>absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testMultipleJobDetails() {
    String updateId1 = "u1";
    String updateId2 = "u2";
    IJobUpdateDetails details1 =
        makeJobDetails(makeJobUpdate(JobKeys.from("role", "env", "name1"), updateId1));
    IJobUpdateDetails details2 =
        makeJobDetails(makeJobUpdate(JobKeys.from("role", "env", "name2"), updateId2));

    assertEquals(ImmutableList.<IJobInstanceUpdateEvent>of(), getInstanceEvents(updateId2, 3));

    saveUpdate(details1.getUpdate(), Optional.of("lock1"));
    saveUpdate(details2.getUpdate(), Optional.of("lock2"));

    details1 = updateJobDetails(populateExpected(details1.getUpdate()), FIRST_EVENT);
    details2 = updateJobDetails(populateExpected(details2.getUpdate()), FIRST_EVENT);
    assertEquals(Optional.of(details1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(details2), getUpdateDetails(updateId2));

    IJobUpdateEvent jEvent11 = makeJobUpdateEvent(ROLLING_FORWARD, 456L);
    IJobUpdateEvent jEvent12 = makeJobUpdateEvent(ERROR, 457L);
    IJobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, INSTANCE_UPDATED);
    IJobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 452L, INSTANCE_UPDATING);

    IJobUpdateEvent jEvent21 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 567L);
    IJobUpdateEvent jEvent22 = makeJobUpdateEvent(ABORTED, 568L);
    IJobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, INSTANCE_UPDATING);
    IJobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(3, 562L, INSTANCE_UPDATED);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);
    assertEquals(ImmutableList.<IJobInstanceUpdateEvent>of(), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent21, updateId2);

    assertEquals(ImmutableList.of(iEvent21), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent22, updateId2);
    assertEquals(ImmutableList.of(iEvent21, iEvent22), getInstanceEvents(updateId2, 3));

    details1 = updateJobDetails(
        populateExpected(details1.getUpdate(), ERROR, CREATED_MS, 457L),
        ImmutableList.of(FIRST_EVENT, jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    details2 = updateJobDetails(
        populateExpected(details2.getUpdate(), ABORTED, CREATED_MS, 568L),
        ImmutableList.of(FIRST_EVENT, jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    assertEquals(Optional.of(details1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(details2), getUpdateDetails(updateId2));

    assertEquals(
        ImmutableSet.of(
            new StoredJobUpdateDetails(details1.newBuilder(), "lock1"),
            new StoredJobUpdateDetails(details2.newBuilder(), "lock2")),
        getAllUpdateDetails());
  }

  @Test
  public void testTruncateJobUpdates() {
    String updateId = "u5";
    IJobUpdate update = makeJobUpdate(JOB, updateId);
    IJobUpdateEvent updateEvent = IJobUpdateEvent.build(new JobUpdateEvent(ROLLING_FORWARD, 123L));
    IJobInstanceUpdateEvent instanceEvent = IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(0, 125L, INSTANCE_ROLLBACK_FAILED));

    saveUpdate(update, Optional.of("lock"));
    saveJobEvent(updateEvent, updateId);
    saveJobInstanceEvent(instanceEvent, updateId);
    assertEquals(
        populateExpected(update, ROLLING_FORWARD, CREATED_MS, 125L),
        getUpdate(updateId).get());
    assertEquals(2, getUpdateDetails(updateId).get().getUpdateEvents().size());
    assertEquals(1, getUpdateDetails(updateId).get().getInstanceEvents().size());

    truncateUpdates();
    assertEquals(Optional.<IJobUpdateDetails>absent(), getUpdateDetails(updateId));
  }

  @Test
  public void testPruneHistory() {
    String updateId1 = "u11";
    String updateId2 = "u12";
    String updateId3 = "u13";
    String updateId4 = "u14";
    String updateId5 = "u15";
    String updateId6 = "u16";
    String updateId7 = "u17";

    IJobKey job2 = JobKeys.from("testRole2", "testEnv2", "job2");
    IJobUpdate update1 = makeJobUpdate(JOB, updateId1);
    IJobUpdate update2 = makeJobUpdate(JOB, updateId2);
    IJobUpdate update3 = makeJobUpdate(JOB, updateId3);
    IJobUpdate update4 = makeJobUpdate(JOB, updateId4);
    IJobUpdate update5 = makeJobUpdate(job2, updateId5);
    IJobUpdate update6 = makeJobUpdate(job2, updateId6);
    IJobUpdate update7 = makeJobUpdate(job2, updateId7);

    IJobUpdateEvent updateEvent1 = IJobUpdateEvent.build(new JobUpdateEvent(ROLLING_BACK, 123L));
    IJobUpdateEvent updateEvent2 = IJobUpdateEvent.build(new JobUpdateEvent(ABORTED, 124L));
    IJobUpdateEvent updateEvent3 = IJobUpdateEvent.build(new JobUpdateEvent(ROLLED_BACK, 125L));
    IJobUpdateEvent updateEvent4 = IJobUpdateEvent.build(new JobUpdateEvent(FAILED, 126L));
    IJobUpdateEvent updateEvent5 = IJobUpdateEvent.build(new JobUpdateEvent(ERROR, 123L));
    IJobUpdateEvent updateEvent6 = IJobUpdateEvent.build(new JobUpdateEvent(FAILED, 125L));
    IJobUpdateEvent updateEvent7 = IJobUpdateEvent.build(new JobUpdateEvent(ROLLING_FORWARD, 126L));

    update1 = populateExpected(
        saveUpdateNoEvent(update1, Optional.of("lock1")), ROLLING_BACK, 123L, 123L);
    update2 = populateExpected(
        saveUpdateNoEvent(update2, Optional.<String>absent()), ABORTED, 124L, 124L);
    update3 = populateExpected(
        saveUpdateNoEvent(update3, Optional.<String>absent()), ROLLED_BACK, 125L, 125L);
    update4 = populateExpected(
        saveUpdateNoEvent(update4, Optional.<String>absent()), FAILED, 126L, 126L);
    update5 = populateExpected(
        saveUpdateNoEvent(update5, Optional.<String>absent()), ERROR, 123L, 123L);
    update6 = populateExpected(
        saveUpdateNoEvent(update6, Optional.<String>absent()), FAILED, 125L, 125L);
    update7 = populateExpected(
        saveUpdateNoEvent(update7, Optional.of("lock2")), ROLLING_FORWARD, 126L, 126L);

    saveJobEvent(updateEvent1, updateId1);
    saveJobEvent(updateEvent2, updateId2);
    saveJobEvent(updateEvent3, updateId3);
    saveJobEvent(updateEvent4, updateId4);
    saveJobEvent(updateEvent5, updateId5);
    saveJobEvent(updateEvent6, updateId6);
    saveJobEvent(updateEvent7, updateId7);

    assertEquals(update1, getUpdate(updateId1).get());
    assertEquals(update2, getUpdate(updateId2).get());
    assertEquals(update3, getUpdate(updateId3).get());
    assertEquals(update4, getUpdate(updateId4).get());
    assertEquals(update5, getUpdate(updateId5).get());
    assertEquals(update6, getUpdate(updateId6).get());
    assertEquals(update7, getUpdate(updateId7).get());

    long pruningThreshold = 120L;

    // No updates pruned.
    assertEquals(ImmutableSet.<String>of(), pruneHistory(3, pruningThreshold));
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.of(update6), getUpdate(updateId6));
    assertEquals(Optional.of(update5), getUpdate(updateId5));

    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.of(update3), getUpdate(updateId3));
    assertEquals(Optional.of(update2), getUpdate(updateId2));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    assertEquals(ImmutableSet.of(updateId2), pruneHistory(2, pruningThreshold));
    // No updates pruned.
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.of(update6), getUpdate(updateId6));
    assertEquals(Optional.of(update5), getUpdate(updateId5));

    // 1 update pruned.
    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.of(update3), getUpdate(updateId3));
    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId2));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    assertEquals(ImmutableSet.of(updateId5, updateId3), pruneHistory(1, pruningThreshold));
    // 1 update pruned.
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.of(update6), getUpdate(updateId6));
    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId5));

    // 2 updates pruned.
    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId3));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    // The oldest update is pruned.
    assertEquals(ImmutableSet.of(updateId6), pruneHistory(1, 126L));
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update
    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId6));

    assertEquals(Optional.of(update4), getUpdate(updateId4));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update

    // Nothing survives the 0 per job count.
    assertEquals(ImmutableSet.of(updateId4), pruneHistory(0, pruningThreshold));
    assertEquals(Optional.of(update7), getUpdate(updateId7)); // active update

    assertEquals(Optional.<IJobUpdate>absent(), getUpdate(updateId4));
    assertEquals(Optional.of(update1), getUpdate(updateId1)); // active update
  }

  @Test(expected = StorageException.class)
  public void testSaveUpdateWithoutLock() {
    final IJobUpdate update = makeJobUpdate(JOB, "updateId");
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdate(update, Optional.of("lock"));
      }
    });
  }

  @Test(expected = StorageException.class)
  public void testSaveTwoUpdatesForOneJob() {
    final IJobUpdate update = makeJobUpdate(JOB, "updateId");
    saveUpdate(update, Optional.of("lock1"));
    saveUpdate(update, Optional.of("lock2"));
  }

  @Test(expected = StorageException.class)
  public void testSaveTwoUpdatesSameJobKey() {
    final IJobUpdate update1 = makeJobUpdate(JOB, "update1");
    final IJobUpdate update2 = makeJobUpdate(JOB, "update2");
    saveUpdate(update1, Optional.of("lock1"));
    saveUpdate(update2, Optional.of("lock1"));
  }

  @Test
  public void testLockCleared() {
    final IJobUpdate update = makeJobUpdate(JOB, "update1");
    saveUpdate(update, Optional.of("lock1"));

    removeLock(update, "lock1");

    assertEquals(
        Optional.of(updateJobDetails(populateExpected(update), FIRST_EVENT)),
        getUpdateDetails("update1"));
    assertEquals(
        ImmutableSet.of(
            new StoredJobUpdateDetails(
                updateJobDetails(populateExpected(update), FIRST_EVENT).newBuilder(),
                null)),
        getAllUpdateDetails());

    assertEquals(
        ImmutableList.of(populateExpected(update).getSummary()),
        getSummaries(new JobUpdateQuery().setUpdateId("update1")));

    // If the lock has been released for this job, we can start another update.
    saveUpdate(makeJobUpdate(JOB, "update2"), Optional.of("lock2"));
  }

  private static final Optional<String> NO_TOKEN = Optional.absent();

  @Test
  public void testGetLockToken() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        final IJobUpdate update1 = makeJobUpdate(JobKeys.from("role", "env", "name1"), "update1");
        final IJobUpdate update2 = makeJobUpdate(JobKeys.from("role", "env", "name2"), "update2");
        saveUpdate(update1, Optional.of("lock1"));
        assertEquals(
            Optional.of("lock1"),
            storeProvider.getJobUpdateStore().getLockToken("update1"));
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken("update2"));

        saveUpdate(update2, Optional.of("lock2"));
        assertEquals(
            Optional.of("lock1"),
            storeProvider.getJobUpdateStore().getLockToken("update1"));
        assertEquals(
            Optional.of("lock2"),
            storeProvider.getJobUpdateStore().getLockToken("update2"));

        storeProvider.getLockStore().removeLock(
            makeLock(update1.getSummary().getJobKey(), "lock1").getKey());
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken("update1"));
        assertEquals(
            Optional.of("lock2"),
            storeProvider.getJobUpdateStore().getLockToken("update2"));

        storeProvider.getLockStore().removeLock(
            makeLock(update2.getSummary().getJobKey(), "lock2").getKey());
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken("update1"));
        assertEquals(NO_TOKEN, storeProvider.getJobUpdateStore().getLockToken("update2"));
      }
    });
  }

  @Test
  public void testGetSummaries() {
    String role1 = "role1";
    IJobKey job1 = JobKeys.from(role1, "env", "name1");
    IJobKey job2 = JobKeys.from(role1, "env", "name2");
    IJobKey job3 = JobKeys.from(role1, "env", "name3");
    IJobKey job4 = JobKeys.from(role1, "env", "name4");
    IJobKey job5 = JobKeys.from("role", "env", "name5");
    IJobUpdateSummary s1 =
        saveSummary(job1, "u1", 1230L, ROLLED_BACK, "user", Optional.of("lock1"));
    IJobUpdateSummary s2 =  saveSummary(job2, "u2", 1231L, ABORTED, "user", Optional.of("lock2"));
    IJobUpdateSummary s3 = saveSummary(job3, "u3", 1239L, ERROR, "user2", Optional.of("lock3"));
    IJobUpdateSummary s4 =
        saveSummary(job4, "u4", 1234L, ROLL_BACK_PAUSED, "user3", Optional.of("lock4"));
    IJobUpdateSummary s5 =
        saveSummary(job5, "u5", 1235L, ROLLING_FORWARD, "user4", Optional.of("lock5"));

    // Test empty query returns all.
    assertEquals(ImmutableList.of(s3, s5, s4, s2, s1), getSummaries(new JobUpdateQuery()));

    // Test query by updateId.
    assertEquals(ImmutableList.of(s1), getSummaries(new JobUpdateQuery().setUpdateId("u1")));

    // Test query by role.
    assertEquals(
        ImmutableList.of(s3, s4, s2, s1),
        getSummaries(new JobUpdateQuery().setRole(role1)));

    // Test query by job key.
    assertEquals(
        ImmutableList.of(s5),
        getSummaries(new JobUpdateQuery().setJobKey(job5.newBuilder())));

    // Test query by user.
    assertEquals(ImmutableList.of(s2, s1), getSummaries(new JobUpdateQuery().setUser("user")));

    // Test query by one status.
    assertEquals(ImmutableList.of(s3), getSummaries(new JobUpdateQuery().setUpdateStatuses(
        ImmutableSet.of(ERROR))));

    // Test query by multiple statuses.
    assertEquals(ImmutableList.of(s3, s2, s1), getSummaries(new JobUpdateQuery().setUpdateStatuses(
        ImmutableSet.of(ERROR, ABORTED, ROLLED_BACK))));

    // Test query by empty statuses.
    assertEquals(
        ImmutableList.of(s3, s5, s4, s2, s1),
        getSummaries(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.<JobUpdateStatus>of())));

    // Test paging.
    assertEquals(
        ImmutableList.of(s3, s5),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(0)));
    assertEquals(
        ImmutableList.of(s4, s2),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(2)));
    assertEquals(
        ImmutableList.of(s1),
        getSummaries(new JobUpdateQuery().setLimit(2).setOffset(4)));

    // Test no match.
    assertEquals(
        ImmutableList.<IJobUpdateSummary>of(),
        getSummaries(new JobUpdateQuery().setRole("no_match")));
  }

  private void assertUpdate(IJobUpdate expected) {
    String updateId = expected.getSummary().getUpdateId();
    assertEquals(populateExpected(expected), getUpdate(updateId).get());
    assertEquals(getUpdate(updateId).get(), getUpdateDetails(updateId).get().getUpdate());
    assertEquals(getUpdateInstructions(updateId).get(), expected.getInstructions());
  }

  private Optional<IJobUpdate> getUpdate(final String updateId) {
    return storage.consistentRead(new Quiet<Optional<IJobUpdate>>() {
      @Override
      public Optional<IJobUpdate> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdate(updateId);
      }
    });
  }

  private List<IJobInstanceUpdateEvent> getInstanceEvents(final String updateId, final int id) {
    return storage.consistentRead(new Quiet<List<IJobInstanceUpdateEvent>>() {
      @Override
      public List<IJobInstanceUpdateEvent> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchInstanceEvents(updateId, id);
      }
    });
  }

  private Optional<IJobUpdateInstructions> getUpdateInstructions(final String updateId) {
    return storage.consistentRead(new Quiet<Optional<IJobUpdateInstructions>>() {
      @Override
      public Optional<IJobUpdateInstructions> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateInstructions(updateId);
      }
    });
  }

  private Optional<IJobUpdateDetails> getUpdateDetails(final String updateId) {
    return storage.consistentRead(new Quiet<Optional<IJobUpdateDetails>>() {
      @Override
      public Optional<IJobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().fetchJobUpdateDetails(updateId);
      }
    });
  }

  private Set<StoredJobUpdateDetails> getAllUpdateDetails() {
    return storage.consistentRead(new Quiet<Set<StoredJobUpdateDetails>>() {
      @Override
      public Set<StoredJobUpdateDetails> apply(Storage.StoreProvider storeProvider) {
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

  private static ILock makeLock(IJobKey jobKey, String lockToken) {
    return ILock.build(new Lock()
        .setKey(LockKey.job(jobKey.newBuilder()))
        .setToken(lockToken)
        .setTimestampMs(100)
        .setUser("fake user"));
  }

  private IJobUpdate saveUpdate(final IJobUpdate update, final Optional<String> lockToken) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        if (lockToken.isPresent()) {
          storeProvider.getLockStore().saveLock(
              makeLock(update.getSummary().getJobKey(), lockToken.get()));
        }
        storeProvider.getJobUpdateStore().saveJobUpdate(update, lockToken);
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(
            FIRST_EVENT,
            update.getSummary().getUpdateId());
      }
    });

    return update;
  }

  private IJobUpdate saveUpdateNoEvent(final IJobUpdate update, final Optional<String> lockToken) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        if (lockToken.isPresent()) {
          storeProvider.getLockStore().saveLock(
              makeLock(update.getSummary().getJobKey(), lockToken.get()));
        }
        storeProvider.getJobUpdateStore().saveJobUpdate(update, lockToken);
      }
    });

    return update;
  }

  private void saveJobEvent(final IJobUpdateEvent event, final String updateId) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobUpdateEvent(event, updateId);
      }
    });
  }

  private void saveJobInstanceEvent(final IJobInstanceUpdateEvent event, final String updateId) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(event, updateId);
      }
    });
  }

  private void truncateUpdates() {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getJobUpdateStore().deleteAllUpdatesAndEvents();
      }
    });
  }

  private Set<String> pruneHistory(final int retainCount, final long pruningThresholdMs) {
    return storage.write(new MutateWork.Quiet<Set<String>>() {
      @Override
      public Set<String> apply(MutableStoreProvider storeProvider) {
        return storeProvider.getJobUpdateStore().pruneHistory(retainCount, pruningThresholdMs);
      }
    });
  }

  private void removeLock(final IJobUpdate update, final String lockToken) {
    storage.write(new MutateWork.NoResult.Quiet() {
      @Override
      public void execute(MutableStoreProvider storeProvider) {
        storeProvider.getLockStore().removeLock(
            makeLock(update.getSummary().getJobKey(), lockToken).getKey());
      }
    });
  }

  private IJobUpdate populateExpected(IJobUpdate update) {
    return populateExpected(update, ROLLING_FORWARD, CREATED_MS, CREATED_MS);
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

  private static IJobUpdateEvent makeJobUpdateEvent(
      JobUpdateStatus status,
      long timestampMs,
      String user) {

    return IJobUpdateEvent.build(
        new JobUpdateEvent(status, timestampMs).setUser(user));
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
        ImmutableList.of(FIRST_EVENT),
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
      String user,
      Optional<String> lockToken) {

    IJobUpdateSummary summary = IJobUpdateSummary.build(new JobUpdateSummary()
        .setUpdateId(updateId)
        .setJobKey(jobKey.newBuilder())
        .setUser(user));

    IJobUpdate update = makeJobUpdate(summary);
    saveUpdate(update, lockToken);
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
        .setInstructions(makeJobUpdateInstructions().newBuilder()));
  }

  private IJobUpdateInstructions makeJobUpdateInstructions() {
    return IJobUpdateInstructions.build(new JobUpdateInstructions()
        .setDesiredState(new InstanceTaskConfig()
            .setTask(makeTaskConfig())
            .setInstances(ImmutableSet.of(new Range(0, 7), new Range(8, 9))))
        .setInitialState(ImmutableSet.of(
            new InstanceTaskConfig()
                .setInstances(ImmutableSet.of(new Range(0, 1), new Range(2, 3)))
                .setTask(makeTaskConfig()),
            new InstanceTaskConfig()
                .setInstances(ImmutableSet.of(new Range(4, 5), new Range(6, 7)))
                .setTask(makeTaskConfig())))
        .setSettings(new JobUpdateSettings()
            .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 0), new Range(3, 5)))));
  }

  private TaskConfig makeTaskConfig() {
    return new TaskConfig()
        .setJobName(JOB.getName())
        .setEnvironment(JOB.getEnvironment())
        .setOwner(new Identity(JOB.getRole(), "user"))
        .setIsService(true);
  }
}
