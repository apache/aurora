package com.twitter.mesos.scheduler;

import java.util.Set;

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

  public static TaskQuery byId(Iterable<String> taskIds) {
    return new TaskQuery().setTaskIds(ImmutableSet.copyOf(taskIds));
  }

  public static TaskQuery byId(String taskId) {
    return byId(ImmutableSet.of(taskId));
  }

  public static TaskQuery liveShard(String role, String job, int shard) {
    return liveShards(role, job, ImmutableSet.of(shard));
  }

  public static TaskQuery liveShards(String role, String job, Set<Integer> shards) {
    return byJob(role, job)
        .setShardIds(ImmutableSet.copyOf(shards))
        .setStatuses(Tasks.ACTIVE_STATES);
  }

  public static TaskQuery byRole(String roleAccount) {
    return new TaskQuery().setOwner(new Identity().setRole(roleAccount));
  }

  public static TaskQuery byStatus(ScheduleStatus status) {
    return new TaskQuery().setStatuses(ImmutableSet.of(status));
  }

  public static TaskQuery activeQuery(String role, String job) {
    return byJob(role, job).setStatuses(Tasks.ACTIVE_STATES);
  }

  public static TaskQuery activeQuery(Identity owner, String jobName) {
    return activeQuery(owner.getRole(), jobName);
  }

  public static TaskQuery byJob(String role, String job) {
    return new TaskQuery()
        .setOwner(new Identity().setRole(role))
        .setJobName(job);
  }

  /**
   * Checks whether a query is scoped to a specific job.
   * A query scoped to a job specifies a role and job name.
   *
   * @param query Query to test.
   * @return {@code true} if the query specifies at least a role and job name,
   *         otherwise {@code false}.
   */
  public static boolean isJobScoped(TaskQuery query) {
    return (query.getOwner() != null)
        && !isEmpty(query.getOwner().getRole())
        && !isEmpty(query.getJobName());
  }
}
