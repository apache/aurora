package com.twitter.mesos.scheduler;

import java.util.Set;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * A utility class to construct storage queries.
 */
public final class Query {
  public static final TaskQuery GET_ALL = new TaskQuery();

  private Query() {
    // Utility.
  }

  /**
   * Checks whether this query specifies a job by key or owner and name.
   *
   * @param query The query to test.
   * @return {@code true} if this query will filter to a specific job, {@code false} otherwise.
   */
  public static boolean specifiesJob(TaskQuery query) {
    if (query.getOwner() != null
        && !isEmpty(query.getOwner().getRole())
        && !isEmpty(query.getJobName())) {
      return true;
    }
    return !isEmpty(query.getJobKey());
  }

  /**
   * Determines whether this query is only filtering by job.
   *
   * @param query The query to test.
   * @return {@code true} If the only filtering in this query is by job, {@code false} otherwise.
   */
  public static boolean specifiesJobOnly(TaskQuery query) {
    return specifiesJob(query) && (query.getStatusesSize() == 0) && (query.getTaskIdsSize() == 0);
  }

  /**
   * Gets the job key that this query specifies.
   *
   * @param query The query to extract a job key from.
   * @return The job key (explicit or by owner/name) for this query, if specified.
   */
  public static Optional<String> getJobKey(TaskQuery query) {
    if (!specifiesJob(query)) {
      return Optional.absent();
    }

    return Optional.of(isEmpty(query.getJobKey())
          ? Tasks.jobKey(query.getOwner(), query.getJobName()) : query.getJobKey());
  }

  public static TaskQuery byId(Iterable<String> taskIds) {
    return new TaskQuery().setTaskIds(ImmutableSet.copyOf(taskIds));
  }

  public static TaskQuery byId(String taskId) {
    return byId(ImmutableSet.of(taskId));
  }

  public static TaskQuery liveShard(String jobKey, int shard) {
    return liveShards(jobKey, ImmutableSet.of(shard));
  }

  public static TaskQuery liveShards(String jobKey, Set<Integer> shards) {
    return new TaskQuery().setJobKey(jobKey).setShardIds(ImmutableSet.copyOf(shards))
        .setStatuses(Tasks.ACTIVE_STATES);
  }

  public static TaskQuery byRole(String roleAccount) {
    return new TaskQuery().setOwner(new Identity().setRole(roleAccount));
  }

  public static TaskQuery byStatus(ScheduleStatus status) {
    return new TaskQuery().setStatuses(ImmutableSet.of(status));
  }

  public static TaskQuery activeQuery(String jobKey) {
    return new TaskQuery().setJobKey(jobKey).setStatuses(Tasks.ACTIVE_STATES);
  }

  public static TaskQuery activeQuery(Identity owner, String jobName) {
    return activeQuery(Tasks.jobKey(owner, jobName));
  }
}
