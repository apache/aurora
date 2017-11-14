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

package org.apache.aurora.scheduler.storage.mem;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.primitives.Longs;

import org.apache.aurora.common.base.MorePreconditions;
import org.apache.aurora.common.inject.TimedInterceptor.Timed;
import org.apache.aurora.common.stats.StatsProvider;
import org.apache.aurora.gen.JobInstanceUpdateEvent;
import org.apache.aurora.gen.JobUpdateAction;
import org.apache.aurora.gen.JobUpdateDetails;
import org.apache.aurora.gen.JobUpdateEvent;
import org.apache.aurora.gen.JobUpdateState;
import org.apache.aurora.gen.JobUpdateStatus;
import org.apache.aurora.scheduler.storage.JobUpdateStore;
import org.apache.aurora.scheduler.storage.Storage.StorageException;
import org.apache.aurora.scheduler.storage.entities.IJobInstanceUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdate;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateDetails;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateEvent;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateInstructions;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateKey;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateQuery;
import org.apache.aurora.scheduler.storage.entities.IJobUpdateSummary;

import static java.util.Objects.requireNonNull;

import static org.apache.aurora.scheduler.storage.Util.jobUpdateActionStatName;
import static org.apache.aurora.scheduler.storage.Util.jobUpdateStatusStatName;

public class MemJobUpdateStore implements JobUpdateStore.Mutable {

  private static final Ordering<IJobUpdateDetails> REVERSE_LAST_MODIFIED_ORDER = Ordering.natural()
      .reverse()
      .onResultOf(u -> u.getUpdate().getSummary().getState().getLastModifiedTimestampMs());

  private final Map<IJobUpdateKey, IJobUpdateDetails> updates = Maps.newConcurrentMap();
  private final LoadingCache<JobUpdateStatus, AtomicLong> jobUpdateEventStats;
  private final LoadingCache<JobUpdateAction, AtomicLong> jobUpdateActionStats;

  @Inject
  public MemJobUpdateStore(StatsProvider statsProvider) {
    this.jobUpdateEventStats = CacheBuilder.newBuilder()
        .build(new CacheLoader<JobUpdateStatus, AtomicLong>() {
          @Override
          public AtomicLong load(JobUpdateStatus status) {
            return statsProvider.makeCounter(jobUpdateStatusStatName(status));
          }
        });
    for (JobUpdateStatus status : JobUpdateStatus.values()) {
      jobUpdateEventStats.getUnchecked(status).get();
    }
    this.jobUpdateActionStats = CacheBuilder.newBuilder()
        .build(new CacheLoader<JobUpdateAction, AtomicLong>() {
          @Override
          public AtomicLong load(JobUpdateAction action) {
            return statsProvider.makeCounter(jobUpdateActionStatName(action));
          }
        });
    for (JobUpdateAction action : JobUpdateAction.values()) {
      jobUpdateActionStats.getUnchecked(action).get();
    }
  }

  @Timed("job_update_store_fetch_summaries")
  @Override
  public synchronized List<IJobUpdateSummary> fetchJobUpdateSummaries(IJobUpdateQuery query) {
    return performQuery(query)
        .map(u -> u.getUpdate().getSummary())
        .collect(Collectors.toList());
  }

  @Timed("job_update_store_fetch_details_list")
  @Override
  public synchronized List<IJobUpdateDetails> fetchJobUpdateDetails(IJobUpdateQuery query) {
    return performQuery(query).collect(Collectors.toList());
  }

  @Timed("job_update_store_fetch_details")
  @Override
  public synchronized Optional<IJobUpdateDetails> fetchJobUpdateDetails(IJobUpdateKey key) {
    return Optional.fromNullable(updates.get(key));
  }

  @Timed("job_update_store_fetch_update")
  @Override
  public synchronized Optional<IJobUpdate> fetchJobUpdate(IJobUpdateKey key) {
    return Optional.fromNullable(updates.get(key)).transform(IJobUpdateDetails::getUpdate);
  }

  @Timed("job_update_store_fetch_instructions")
  @Override
  public synchronized Optional<IJobUpdateInstructions> fetchJobUpdateInstructions(
      IJobUpdateKey key) {

    return Optional.fromNullable(updates.get(key))
        .transform(u -> u.getUpdate().getInstructions());
  }

  @Timed("job_update_store_fetch_all_details")
  @Override
  public synchronized Set<IJobUpdateDetails> fetchAllJobUpdateDetails() {
    return ImmutableSet.copyOf(updates.values());
  }

  @Timed("job_update_store_fetch_instance_events")
  @Override
  public synchronized List<IJobInstanceUpdateEvent> fetchInstanceEvents(
      IJobUpdateKey key,
      int instanceId) {

    return java.util.Optional.ofNullable(updates.get(key))
        .map(IJobUpdateDetails::getInstanceEvents)
        .orElse(ImmutableList.of())
        .stream()
        .filter(e -> e.getInstanceId() == instanceId)
        .collect(Collectors.toList());
  }

  private static void validateInstructions(IJobUpdateInstructions instructions) {
    if (!instructions.isSetDesiredState() && instructions.getInitialState().isEmpty()) {
      throw new IllegalArgumentException(
          "Missing both initial and desired states. At least one is required.");
    }

    if (!instructions.getInitialState().isEmpty()) {
      if (instructions.getInitialState().stream().anyMatch(t -> t.getTask() == null)) {
        throw new NullPointerException("Invalid initial instance state.");
      }
      Preconditions.checkArgument(
          instructions.getInitialState().stream().noneMatch(t -> t.getInstances().isEmpty()),
          "Invalid intial instance state ranges.");
    }

    if (instructions.getDesiredState() != null) {
      MorePreconditions.checkNotBlank(instructions.getDesiredState().getInstances());
      Preconditions.checkNotNull(instructions.getDesiredState().getTask());
    }
  }

  @Timed("job_update_store_save_update")
  @Override
  public synchronized void saveJobUpdate(IJobUpdate update) {
    requireNonNull(update);
    validateInstructions(update.getInstructions());

    if (updates.containsKey(update.getSummary().getKey())) {
      throw new StorageException("Update already exists: " + update.getSummary().getKey());
    }

    JobUpdateDetails mutable = new JobUpdateDetails()
        .setUpdate(update.newBuilder())
        .setUpdateEvents(ImmutableList.of())
        .setInstanceEvents(ImmutableList.of());
    mutable.getUpdate().getSummary().setState(synthesizeUpdateState(mutable));

    updates.put(update.getSummary().getKey(), IJobUpdateDetails.build(mutable));
  }

  private static final Ordering<JobUpdateEvent> EVENT_ORDERING = Ordering.natural()
      .onResultOf(JobUpdateEvent::getTimestampMs);

  @Timed("job_update_store_save_event")
  @Override
  public synchronized void saveJobUpdateEvent(IJobUpdateKey key, IJobUpdateEvent event) {
    IJobUpdateDetails update = updates.get(key);
    if (update == null) {
      throw new StorageException("Update not found: " + key);
    }

    JobUpdateDetails mutable = update.newBuilder();
    mutable.addToUpdateEvents(event.newBuilder());
    mutable.setUpdateEvents(EVENT_ORDERING.sortedCopy(mutable.getUpdateEvents()));
    mutable.getUpdate().getSummary().setState(synthesizeUpdateState(mutable));
    updates.put(key, IJobUpdateDetails.build(mutable));
    jobUpdateEventStats.getUnchecked(event.getStatus()).incrementAndGet();
  }

  private static final Ordering<JobInstanceUpdateEvent> INSTANCE_EVENT_ORDERING = Ordering.natural()
      .onResultOf(JobInstanceUpdateEvent::getTimestampMs);

  @Timed("job_update_store_save_instance_event")
  @Override
  public synchronized void saveJobInstanceUpdateEvent(
      IJobUpdateKey key,
      IJobInstanceUpdateEvent event) {

    IJobUpdateDetails update = updates.get(key);
    if (update == null) {
      throw new StorageException("Update not found: " + key);
    }

    JobUpdateDetails mutable = update.newBuilder();
    mutable.addToInstanceEvents(event.newBuilder());
    mutable.setInstanceEvents(INSTANCE_EVENT_ORDERING.sortedCopy(mutable.getInstanceEvents()));
    mutable.getUpdate().getSummary().setState(synthesizeUpdateState(mutable));
    updates.put(key, IJobUpdateDetails.build(mutable));
    jobUpdateActionStats.getUnchecked(event.getAction()).incrementAndGet();
  }

  @Timed("job_update_store_delete_all")
  @Override
  public synchronized void deleteAllUpdatesAndEvents() {
    updates.clear();
  }

  @Timed("job_update_store_prune_history")
  @Override
  public synchronized Set<IJobUpdateKey> pruneHistory(
      int perJobRetainCount,
      long historyPruneThresholdMs) {

    Supplier<Stream<IJobUpdateSummary>> completedUpdates = () -> updates.values().stream()
        .map(u -> u.getUpdate().getSummary())
        .filter(s -> TERMINAL_STATES.contains(s.getState().getStatus()));

    Predicate<IJobUpdateSummary> expiredFilter =
        s -> s.getState().getCreatedTimestampMs() < historyPruneThresholdMs;

    ImmutableSet.Builder<IJobUpdateKey> pruneBuilder = ImmutableSet.builder();

    // Gather updates based on time threshold.
    pruneBuilder.addAll(completedUpdates.get()
        .filter(expiredFilter)
        .map(IJobUpdateSummary::getKey)
        .collect(Collectors.toList()));

    Multimap<IJobKey, IJobUpdateSummary> updatesByJob = Multimaps.index(
        // Avoid counting to-be-removed expired updates.
        completedUpdates.get().filter(expiredFilter.negate()).iterator(),
        s -> s.getKey().getJob());

    for (Map.Entry<IJobKey, Collection<IJobUpdateSummary>> entry
        : updatesByJob.asMap().entrySet()) {

      if (entry.getValue().size() > perJobRetainCount) {
        Ordering<IJobUpdateSummary> creationOrder = Ordering.natural()
            .onResultOf(s -> s.getState().getCreatedTimestampMs());
        pruneBuilder.addAll(creationOrder
            .leastOf(entry.getValue(), entry.getValue().size() - perJobRetainCount)
            .stream()
            .map(IJobUpdateSummary::getKey)
            .iterator());
      }
    }

    Set<IJobUpdateKey> pruned = pruneBuilder.build();
    updates.keySet().removeAll(pruned);

    return pruned;
  }

  private static JobUpdateState synthesizeUpdateState(JobUpdateDetails update) {
    JobUpdateState state = update.getUpdate().getSummary().getState();
    if (state == null) {
      state = new JobUpdateState();
    }

    JobUpdateEvent firstEvent = Iterables.getFirst(update.getUpdateEvents(), null);
    if (firstEvent != null) {
      state.setCreatedTimestampMs(firstEvent.getTimestampMs());
    }

    JobUpdateEvent lastEvent = Iterables.getLast(update.getUpdateEvents(), null);
    if (lastEvent != null) {
      state.setStatus(lastEvent.getStatus());
      state.setLastModifiedTimestampMs(lastEvent.getTimestampMs());
    }

    JobInstanceUpdateEvent lastInstanceEvent = Iterables.getLast(update.getInstanceEvents(), null);
    if (lastInstanceEvent != null) {
      state.setLastModifiedTimestampMs(
          Longs.max(state.getLastModifiedTimestampMs(), lastInstanceEvent.getTimestampMs()));
    }

    return state;
  }

  private Stream<IJobUpdateDetails> performQuery(IJobUpdateQuery query) {
    Predicate<IJobUpdateDetails> filter = u -> true;
    if (query.getRole() != null) {
      filter = filter.and(
          u -> u.getUpdate().getSummary().getKey().getJob().getRole().equals(query.getRole()));
    }
    if (query.getKey() != null) {
      filter = filter.and(u -> u.getUpdate().getSummary().getKey().equals(query.getKey()));
    }
    if (query.getJobKey() != null) {
      filter = filter.and(
          u -> u.getUpdate().getSummary().getKey().getJob().equals(query.getJobKey()));
    }
    if (query.getUser() != null) {
      filter = filter.and(u -> u.getUpdate().getSummary().getUser().equals(query.getUser()));
    }
    if (query.getUpdateStatuses() != null && !query.getUpdateStatuses().isEmpty()) {
      filter = filter.and(u -> query.getUpdateStatuses()
          .contains(u.getUpdate().getSummary().getState().getStatus()));
    }

    // TODO(wfarner): Modification time is not a stable ordering for pagination, but we use it as
    // such here.  The behavior is carried over from DbJobupdateStore; determine if it is desired.
    Stream<IJobUpdateDetails> matches = updates.values().stream()
        .filter(filter)
        .sorted(REVERSE_LAST_MODIFIED_ORDER)
        .skip(query.getOffset());

    if (query.getLimit() > 0) {
      matches = matches.limit(query.getLimit());
    }

    return matches;
  }
}
