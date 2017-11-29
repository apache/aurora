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

package org.apache.aurora.scheduler.storage;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Injector;

import org.apache.aurora.gen.InstanceTaskConfig;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdate;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateInstructions;
import org.apache.aurora.gen.JobUpdateKey;
import org.apache.aurora.gen.JobUpdateQuery;
import org.apache.aurora.gen.JobUpdateSettings;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.gen.JobUpdateSummary;
import org.apache.aurora.gen.Metadata;
import org.apache.aurora.gen.Range;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.scheduler.base.JobKeys;
import org.apache.aurora.scheduler.base.TaskTestUtil;
import org.apache.aurora.scheduler.storage.Storage.MutateWork.NoResult;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;
import org.apache.aurora.scheduler.storage.testing.StorageEntityUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATED;
import static org.apache.aurora.gen.JobUpdateAction.INSTANCE_UPDATING;
import static org.apache.aurora.gen.JobUpdateStatus.ABORTED;
import static org.apache.aurora.gen.JobUpdateStatus.ERROR;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLED_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_BACK;
import static org.apache.aurora.gen.JobUpdateStatus.ROLLING_FORWARD;
import static org.apache.aurora.gen.JobUpdateStatus.ROLL_FORWARD_PAUSED;
import static org.apache.aurora.gen.Resource.diskMb;
import static org.apache.aurora.gen.Resource.numCpus;
import static org.apache.aurora.gen.Resource.ramMb;
import static org.junit.Assert.assertEquals;

public abstract class AbstractJobUpdateStoreTest {

  private static final IJobKey JOB = JobKeys.from("testRole", "testEnv", "job");
  private static final long CREATED_MS = 111L;
  private static final IJobUpdateEvent FIRST_EVENT =
      makeJobUpdateEvent(ROLLING_FORWARD, CREATED_MS);
  private static final ImmutableSet<Metadata> METADATA =
      ImmutableSet.of(new Metadata("k1", "v1"), new Metadata("k2", "v2"), new Metadata("k3", "v3"));

  private Storage storage;

  @Before
  public void setUp() throws Exception {
    Injector injector = createStorageInjector();
    storage = injector.getInstance(Storage.class);
  }

  protected abstract Injector createStorageInjector();

  @After
  public void tearDown() throws Exception {
    truncateUpdates();
  }

  private static IJobUpdateDetails makeFullyPopulatedUpdate(IJobUpdateKey key) {
    JobUpdateDetails builder = makeJobUpdate(key).newBuilder()
        .setUpdateEvents(ImmutableList.of(new JobUpdateEvent()
            .setStatus(ROLLING_FORWARD)
            .setTimestampMs(1)
            .setUser("user")
            .setMessage("message")))
        .setInstanceEvents(ImmutableList.of(new JobInstanceUpdateEvent()
            .setTimestampMs(2)
            .setInstanceId(1)
            .setAction(INSTANCE_UPDATING)));
    JobUpdateInstructions instructions = builder.getUpdate().getInstructions();
    Stream.of(
        instructions.getInitialState().stream()
            .map(InstanceTaskConfig::getInstances)
            .flatMap(Set::stream)
            .collect(Collectors.toSet()),
        instructions.getDesiredState().getInstances(),
        instructions.getSettings().getUpdateOnlyTheseInstances())
        .flatMap(Set::stream)
        .forEach(range -> {
          if (range.getFirst() == 0) {
            range.setFirst(1);
          }
          if (range.getLast() == 0) {
            range.setLast(1);
          }
        });
    return IJobUpdateDetails.build(builder);
  }

  @Test
  public void testSaveJobUpdates() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");

    IJobUpdateDetails update1 = makeFullyPopulatedUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

    assertEquals(Optional.absent(), getUpdate(updateId1));
    assertEquals(Optional.absent(), getUpdate(updateId2));

    StorageEntityUtil.assertFullyPopulated(
        update1,
        StorageEntityUtil.getField(JobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(IJobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(Range.class, "first"),
        StorageEntityUtil.getField(Range.class, "last"));
    saveUpdate(update1);
    assertEquals(withUpdateState(update1, ROLLING_FORWARD, 1, 2), getUpdate(key(update1)).get());

    saveUpdate(update2);
    assertUpdate(update2);
  }

  @Test
  public void testSaveJobUpdateWithLargeTaskConfigValues() {
    // AURORA-1494 regression test validating max resources values are allowed.
    IJobUpdateKey updateId = makeKey(JobKeys.from("role", "env", "name1"), "u1");

    JobUpdateDetails builder = makeFullyPopulatedUpdate(updateId).newBuilder();
    builder.getUpdate().getInstructions().getDesiredState().getTask().setResources(
            ImmutableSet.of(
                    numCpus(Double.MAX_VALUE),
                    ramMb(Long.MAX_VALUE),
                    diskMb(Long.MAX_VALUE)));

    IJobUpdateDetails update = makeFullyPopulatedUpdate(updateId);
    StorageEntityUtil.assertFullyPopulated(
        update,
        StorageEntityUtil.getField(JobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(IJobUpdateSummary.class, "state"),
        StorageEntityUtil.getField(Range.class, "first"),
        StorageEntityUtil.getField(Range.class, "last"));
    saveUpdate(update);
    assertEquals(withUpdateState(update, ROLLING_FORWARD, 1, 2), getUpdate(key(update)).get());
  }

  @Test
  public void testSaveNullInitialState() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().unsetInitialState();

    // Save with null initial state instances.
    saveUpdate(IJobUpdateDetails.build(builder));

    builder.getUpdate().getInstructions().setInitialState(ImmutableSet.of());
    assertUpdate(IJobUpdateDetails.build(builder));
  }

  @Test
  public void testSaveNullDesiredState() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().unsetDesiredState();

    // Save with null desired state instances.
    saveUpdate(IJobUpdateDetails.build(builder));

    assertUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveBothInitialAndDesiredMissingThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().unsetInitialState();
    builder.getUpdate().getInstructions().unsetDesiredState();

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullInitialStateTaskThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getInitialState().add(
        new InstanceTaskConfig(null, ImmutableSet.of()));

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyInitialStateRangesThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getInitialState().add(
        new InstanceTaskConfig(
            TaskTestUtil.makeConfig(TaskTestUtil.JOB).newBuilder(),
            ImmutableSet.of()));

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = NullPointerException.class)
  public void testSaveNullDesiredStateTaskThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getDesiredState().setTask(null);

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveEmptyDesiredStateRangesThrows() {
    JobUpdateDetails builder = makeJobUpdate(makeKey("u1")).newBuilder();
    builder.getUpdate().getInstructions().getDesiredState().setInstances(ImmutableSet.of());

    saveUpdate(IJobUpdateDetails.build(builder));
  }

  @Test
  public void testSaveJobUpdateEmptyInstanceOverrides() {
    IJobUpdateKey updateId = makeKey("u1");

    JobUpdateDetails builder = makeJobUpdate(updateId).newBuilder();
    builder.getUpdate().getInstructions().getSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of());

    IJobUpdateDetails expected = IJobUpdateDetails.build(builder);

    // Save with empty overrides.
    saveUpdate(expected);
    assertUpdate(expected);
  }

  @Test
  public void testSaveJobUpdateNullInstanceOverrides() {
    IJobUpdateKey updateId = makeKey("u1");

    JobUpdateDetails builder = makeJobUpdate(updateId).newBuilder();
    builder.getUpdate().getInstructions().getSettings()
        .setUpdateOnlyTheseInstances(ImmutableSet.of());

    IJobUpdateDetails expected = IJobUpdateDetails.build(builder);

    // Save with null overrides.
    builder.getUpdate().getInstructions().getSettings().setUpdateOnlyTheseInstances(null);
    saveUpdate(IJobUpdateDetails.build(builder));
    assertUpdate(expected);
  }

  @Test
  public void testSaveJobUpdateStateIgnored() {
    IJobUpdateKey updateId = makeKey("u1");
    IJobUpdateDetails update = withUpdateState(makeJobUpdate(updateId), ABORTED, 567L, 567L);
    saveUpdate(update);

    // Assert state fields were ignored.
    assertUpdate(withDefaultUpdateState(update));
  }

  @Test
  public void testMultipleJobDetails() {
    IJobUpdateKey updateId1 = makeKey(JobKeys.from("role", "env", "name1"), "u1");
    IJobUpdateKey updateId2 = makeKey(JobKeys.from("role", "env", "name2"), "u2");
    IJobUpdateDetails update1 = makeJobUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

    saveUpdate(update1);
    saveUpdate(update2);

    update1 = updateJobDetails(withDefaultUpdateState(update1), FIRST_EVENT);
    update2 = updateJobDetails(withDefaultUpdateState(update2), FIRST_EVENT);
    assertEquals(Optional.of(update1), getUpdateDetails(updateId1));
    assertEquals(Optional.of(update2), getUpdateDetails(updateId2));

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
    assertEquals(ImmutableList.of(), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent21, updateId2);

    assertEquals(ImmutableList.of(iEvent21), getInstanceEvents(updateId2, 3));
    saveJobInstanceEvent(iEvent22, updateId2);
    assertEquals(ImmutableList.of(iEvent21, iEvent22), getInstanceEvents(updateId2, 3));

    update1 = updateJobDetails(
        withUpdateState(update1, ERROR, CREATED_MS, 457L),
        ImmutableList.of(FIRST_EVENT, jEvent11, jEvent12), ImmutableList.of(iEvent11, iEvent12));

    update2 = updateJobDetails(
        withUpdateState(update2, ABORTED, CREATED_MS, 568L),
        ImmutableList.of(FIRST_EVENT, jEvent21, jEvent22), ImmutableList.of(iEvent21, iEvent22));

    assertEquals(
        ImmutableList.of(update2, update1),
        fetchUpdates(JobUpdateStore.MATCH_ALL.newBuilder()));

    assertEquals(
        ImmutableList.of(getUpdateDetails(updateId2).get(), getUpdateDetails(updateId1).get()),
        fetchUpdates(new JobUpdateQuery().setRole("role")));
  }

  @Test
  public void testTruncateJobUpdates() {
    saveUpdate(makeJobUpdate(makeKey("u1")));
    saveUpdate(makeJobUpdate(makeKey("u2")));
    assertEquals(2, fetchUpdates(new JobUpdateQuery()).size());
    truncateUpdates();
    assertEquals(0, fetchUpdates(new JobUpdateQuery()).size());
    truncateUpdates();
    assertEquals(0, fetchUpdates(new JobUpdateQuery()).size());
  }

  @Test
  public void testRemoveUpdates() {
    IJobUpdateDetails u1 = makeJobUpdate(makeKey("u1"));
    IJobUpdateDetails u2 = makeJobUpdate(makeKey("u2"));
    IJobUpdateDetails u3 = makeJobUpdate(makeKey("u3"));
    saveUpdate(u1);
    saveUpdate(u2);
    saveUpdate(u3);

    storage.write((NoResult.Quiet) store ->
        store.getJobUpdateStore().removeJobUpdates(ImmutableSet.of(key(u1), key(u3))));
    assertQueryMatches(new JobUpdateQuery(), withDefaultUpdateState(u2));

    // Noop - update already deleted
    storage.write((NoResult.Quiet) store ->
        store.getJobUpdateStore().removeJobUpdates(ImmutableSet.of(key(u1))));
    assertQueryMatches(new JobUpdateQuery(), withDefaultUpdateState(u2));

    storage.write((NoResult.Quiet) store ->
        store.getJobUpdateStore().removeJobUpdates(ImmutableSet.of(key(u2))));
    assertQueryMatches(new JobUpdateQuery());
  }

  @Test
  public void testSaveJobUpdateWithDuplicateMetadataKeys() {
    IJobUpdateKey updateId = makeKey(JobKeys.from("role", "env", "name1"), "u1");

    ImmutableSet<Metadata> duplicatedMetadata =
        ImmutableSet.of(new Metadata("k1", "v1"), new Metadata("k1", "v2"));
    JobUpdateDetails builder = makeJobUpdate(updateId).newBuilder();
    builder.getUpdate().getSummary().setMetadata(duplicatedMetadata);

    assertEquals(Optional.absent(), getUpdate(updateId));

    IJobUpdateDetails update = IJobUpdateDetails.build(builder);
    saveUpdate(update);
    assertUpdate(update);
  }

  @Test
  public void testQueryDetails() {
    IJobKey jobKey1 = JobKeys.from("role1", "env", "name1");
    IJobUpdateKey updateId1 = makeKey(jobKey1, "u1");
    IJobKey jobKey2 = JobKeys.from("role2", "env", "name2");
    IJobUpdateKey updateId2 = makeKey(jobKey2, "u2");

    IJobUpdateDetails update1 = makeJobUpdate(updateId1);
    IJobUpdateDetails update2 = makeJobUpdate(updateId2);

    saveUpdate(update1);
    saveUpdate(update2);

    updateJobDetails(withDefaultUpdateState(update1), FIRST_EVENT);
    updateJobDetails(withDefaultUpdateState(update2), FIRST_EVENT);

    IJobUpdateEvent jEvent11 = makeJobUpdateEvent(ROLLING_BACK, 450L);
    IJobUpdateEvent jEvent12 = makeJobUpdateEvent(ROLLED_BACK, 500L);
    IJobInstanceUpdateEvent iEvent11 = makeJobInstanceEvent(1, 451L, INSTANCE_ROLLING_BACK);
    IJobInstanceUpdateEvent iEvent12 = makeJobInstanceEvent(2, 458L, INSTANCE_ROLLED_BACK);

    IJobUpdateEvent jEvent21 = makeJobUpdateEvent(ROLL_FORWARD_PAUSED, 550L);
    IJobUpdateEvent jEvent22 = makeJobUpdateEvent(ABORTED, 600L);
    IJobInstanceUpdateEvent iEvent21 = makeJobInstanceEvent(3, 561L, INSTANCE_UPDATING);
    IJobInstanceUpdateEvent iEvent22 = makeJobInstanceEvent(3, 570L, INSTANCE_UPDATED);

    saveJobEvent(jEvent11, updateId1);
    saveJobEvent(jEvent12, updateId1);
    saveJobInstanceEvent(iEvent11, updateId1);
    saveJobInstanceEvent(iEvent12, updateId1);

    saveJobEvent(jEvent21, updateId2);
    saveJobEvent(jEvent22, updateId2);

    saveJobInstanceEvent(iEvent21, updateId2);
    saveJobInstanceEvent(iEvent22, updateId2);

    // Fetch the updates for checking query results.  This avoids the need to manually populate
    // any fields filled by the storage, which is out of scope for this test case.
    update1 = getUpdateDetails(updateId1).get();
    update2 = getUpdateDetails(updateId2).get();

    // Empty query returns all.
    assertQueryMatches(new JobUpdateQuery(), update2, update1);

    // Query by update ID.
    assertQueryMatches(new JobUpdateQuery().setKey(updateId1.newBuilder()), update1);

    // Query by role.
    assertQueryMatches(new JobUpdateQuery().setRole(jobKey2.getRole()), update2);

    // Query by job key.
    assertQueryMatches(new JobUpdateQuery().setJobKey(jobKey2.newBuilder()), update2);

    // Query by status.
    assertQueryMatches(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of(ABORTED)), update2);

    // No match.
    assertQueryMatches(new JobUpdateQuery().setRole("no match"));

    // Querying by incorrect update keys.
    assertQueryMatches(new JobUpdateQuery().setJobKey(JobKeys.from("a", "b", "c").newBuilder()));

    // Query by multiple statuses.
    assertQueryMatches(
        new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of(ABORTED, ROLLING_FORWARD)),
        update2);

    // Query by empty statuses.
    assertQueryMatches(new JobUpdateQuery().setUpdateStatuses(ImmutableSet.of()), update2, update1);

    // Query by user.
    assertQueryMatches(new JobUpdateQuery().setUser("user"), update2, update1);

    // Test paging.
    assertQueryMatches(new JobUpdateQuery().setLimit(1).setOffset(0), update2);
    assertQueryMatches(new JobUpdateQuery().setLimit(2).setOffset(0), update2, update1);
    assertQueryMatches(new JobUpdateQuery().setLimit(3).setOffset(0), update2, update1);
    assertQueryMatches(new JobUpdateQuery().setLimit(1).setOffset(1), update1);
    assertQueryMatches(new JobUpdateQuery().setLimit(2).setOffset(4));
  }

  private static IJobUpdateKey key(IJobUpdateDetails update) {
    return update.getUpdate().getSummary().getKey();
  }

  private void assertQueryMatches(JobUpdateQuery query, IJobUpdateDetails... matches) {
    assertEquals(
        ImmutableList.copyOf(matches),
        storage.read(store ->
            store.getJobUpdateStore().fetchJobUpdates(IJobUpdateQuery.build(query))));
  }

  private static IJobUpdateKey makeKey(String id) {
    return makeKey(JOB, id);
  }

  private static IJobUpdateKey makeKey(IJobKey job, String id) {
    return IJobUpdateKey.build(new JobUpdateKey(job.newBuilder(), id));
  }

  private void assertUpdate(IJobUpdateDetails expected) {
    assertEquals(withDefaultUpdateState(expected), getUpdate(key(expected)).get());
  }

  private Optional<IJobUpdateDetails> getUpdate(IJobUpdateKey key) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(key));
  }

  private List<IJobInstanceUpdateEvent> getInstanceEvents(IJobUpdateKey key, int id) {
    IJobUpdateDetails update =
        storage.read(store -> store.getJobUpdateStore().fetchJobUpdate(key).get());
    return update.getInstanceEvents().stream()
        .filter(e -> e.getInstanceId() == id)
        .collect(Collectors.toList());
  }

  private Optional<IJobUpdateDetails> getUpdateDetails(IJobUpdateKey key) {
    return storage.read(storeProvider -> storeProvider.getJobUpdateStore().fetchJobUpdate(key));
  }

  private List<IJobUpdateDetails> fetchUpdates(JobUpdateQuery query) {
    return storage.read(storeProvider ->
        storeProvider.getJobUpdateStore().fetchJobUpdates(IJobUpdateQuery.build(query)));
  }

  private void saveUpdate(IJobUpdateDetails update) {
    storage.write((NoResult.Quiet) storeProvider -> {
      JobUpdateStore.Mutable store = storeProvider.getJobUpdateStore();
      store.saveJobUpdate(update.getUpdate());
      IJobUpdateKey key = update.getUpdate().getSummary().getKey();
      update.getUpdateEvents().forEach(event -> {
        store.saveJobUpdateEvent(key, event);
      });
      update.getInstanceEvents().forEach(event -> {
        store.saveJobInstanceUpdateEvent(key, event);
      });
    });
  }

  private void saveJobEvent(IJobUpdateEvent event, IJobUpdateKey key) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().saveJobUpdateEvent(key, event));
  }

  private void saveJobInstanceEvent(IJobInstanceUpdateEvent event, IJobUpdateKey key) {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().saveJobInstanceUpdateEvent(key, event));
  }

  private void truncateUpdates() {
    storage.write((NoResult.Quiet)
        storeProvider -> storeProvider.getJobUpdateStore().deleteAllUpdates());
  }

  private IJobUpdateDetails withDefaultUpdateState(IJobUpdateDetails update) {
    return withUpdateState(update, ROLLING_FORWARD, CREATED_MS, CREATED_MS);
  }

  private IJobUpdateDetails withUpdateState(
      IJobUpdateDetails update,
      JobUpdateStatus status,
      long createdMs,
      long lastMs) {

    JobUpdateState state = new JobUpdateState()
        .setCreatedTimestampMs(createdMs)
        .setLastModifiedTimestampMs(lastMs)
        .setStatus(status);
    JobUpdateDetails builder = update.newBuilder();
    builder.getUpdate().getSummary().setState(state);
    return IJobUpdateDetails.build(builder);
  }

  private static IJobUpdateEvent makeJobUpdateEvent(JobUpdateStatus status, long timestampMs) {
    return IJobUpdateEvent.build(
        new JobUpdateEvent(status, timestampMs)
            .setUser("user")
            .setMessage("message"));
  }

  private IJobInstanceUpdateEvent makeJobInstanceEvent(
      int instanceId,
      long timestampMs,
      JobUpdateAction action) {

    return IJobInstanceUpdateEvent.build(
        new JobInstanceUpdateEvent(instanceId, timestampMs, action));
  }

  private IJobUpdateDetails updateJobDetails(IJobUpdateDetails update, IJobUpdateEvent event) {
    return updateJobDetails(
        update,
        ImmutableList.of(event),
        ImmutableList.of());
  }

  private IJobUpdateDetails updateJobDetails(
      IJobUpdateDetails update,
      List<IJobUpdateEvent> jobEvents,
      List<IJobInstanceUpdateEvent> instanceEvents) {

    return IJobUpdateDetails.build(update.newBuilder()
        .setUpdateEvents(IJobUpdateEvent.toBuildersList(jobEvents))
        .setInstanceEvents(IJobInstanceUpdateEvent.toBuildersList(instanceEvents)));
  }

  private static IJobUpdateSummary makeSummary(IJobUpdateKey key, String user) {
    return IJobUpdateSummary.build(new JobUpdateSummary()
        .setKey(key.newBuilder())
        .setUser(user)
        .setMetadata(METADATA));
  }

  private static IJobUpdateDetails makeJobUpdate(IJobUpdateKey key) {
    return IJobUpdateDetails.build(new JobUpdateDetails()
        .setUpdateEvents(ImmutableList.of(FIRST_EVENT.newBuilder()))
        .setUpdate(new JobUpdate()
            .setInstructions(makeJobUpdateInstructions().newBuilder())
            .setSummary(makeSummary(key, "user").newBuilder())));
  }

  private static IJobUpdateInstructions makeJobUpdateInstructions() {
    TaskConfig config = TaskTestUtil.makeConfig(JOB).newBuilder();
    return IJobUpdateInstructions.build(new JobUpdateInstructions()
        .setDesiredState(new InstanceTaskConfig()
            .setTask(config)
            .setInstances(ImmutableSet.of(new Range(0, 7), new Range(8, 9))))
        .setInitialState(ImmutableSet.of(
            new InstanceTaskConfig()
                .setInstances(ImmutableSet.of(new Range(0, 1), new Range(2, 3)))
                .setTask(config),
            new InstanceTaskConfig()
                .setInstances(ImmutableSet.of(new Range(4, 5), new Range(6, 7)))
                .setTask(config)))
        .setSettings(new JobUpdateSettings()
            .setBlockIfNoPulsesAfterMs(500)
            .setUpdateGroupSize(1)
            .setMaxPerInstanceFailures(1)
            .setMaxFailedInstances(1)
            .setMinWaitInInstanceRunningMs(200)
            .setRollbackOnFailure(true)
            .setWaitForBatchCompletion(true)
            .setUpdateOnlyTheseInstances(ImmutableSet.of(new Range(0, 0), new Range(3, 5)))));
  }
}
