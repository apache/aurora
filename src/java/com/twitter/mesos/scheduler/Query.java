package com.twitter.mesos.scheduler;

import java.util.EnumSet;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import com.twitter.common.base.Supplier;

import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.Identity;
import com.twitter.mesos.gen.JobKey;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * A utility class to construct storage queries.
 */
public final class Query {
  public static final TaskQuery GET_ALL = Query.unscoped().get();

  private Query() {
    // Utility.
  }

  public static TaskQuery byId(Iterable<String> taskIds) {
    return taskScoped(taskIds).get();
  }

  public static TaskQuery byId(String taskId, String... taskIds) {
    return taskScoped(taskId, taskIds).get();
  }

  public static TaskQuery byRole(String roleAccount) {
    return roleScoped(roleAccount).get();
  }

  public static TaskQuery byStatus(ScheduleStatus status, ScheduleStatus... statuses) {
    return unscoped().byStatus(status, statuses).get();
  }

  public static TaskQuery byStatus(Iterable<ScheduleStatus> statuses) {
    return unscoped().byStatus(statuses).get();
  }

  public static TaskQuery bySlave(String slaveHost) {
    return slaveScoped(slaveHost).get();
  }

  /**
   * TODO(ksweeney): Deprecate and use JobKey.
   *
   * @deprecated Use {@link #byJob(com.twitter.mesos.gen.JobKey)} instead.
   */
  @Deprecated
  public static TaskQuery byJob(String role, String job) {
    return jobScoped(role, job).get();
  }

  public static TaskQuery byJob(JobKey jobKey) {
    return jobScoped(jobKey).get();
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

  public static Builder unscoped() {
    return new Builder();
  }

  public static Builder roleScoped(String role) {
    return unscoped().byRole(role);
  }

  /**
   * Returns a new Builder scoped to the given job. A builder can only be scoped to a job, a set
   * of shards, or a role once.
   *
   * @deprecated Use {@link #jobScoped(com.twitter.mesos.gen.JobKey)}
   * @param role The role of the job to scope the query to.
   * @param jobName The name of the job to scope the query to.
   * @return A new Builder scoped to the given job.
   */
  @Deprecated
  public static Builder jobScoped(String role, String jobName) {
    return unscoped().byJob(role, jobName);
  }

  public static Builder jobScoped(JobKey jobKey) {
    return unscoped().byJob(jobKey);
  }

  /**
   * Returns a new Builder scoped to the given shardIds of the given job. A builder can only
   * be scoped to a set of shards, a job, or a role once.
   *
   * @deprecated Use {@link #shardScoped(com.twitter.mesos.gen.JobKey, int, int...)}.
   * @param role The role of the job of the shardIds.
   * @param jobName The name of the job of the shardIds.
   * @param shardId A shardId of the target job.
   * @param shardIds Additional shardsIds of the target job.
   * @return A new Builder scoped to the given shardIds.
   */
  @Deprecated
  public static Builder shardScoped(String role, String jobName, int shardId, int... shardIds) {
    return unscoped().byShards(role, jobName, shardId, shardIds);
  }

  public static Builder shardScoped(JobKey jobKey, int shardId, int... shardIds) {
    return unscoped().byShards(jobKey, shardId, shardIds);
  }

  /**
   * Returns a new Builder scoped to the given shardIds of the given job. A builder can only
   * be scoped to a set of shards, a job, or a role once.
   *
   * @deprecated Use {@link #shardScoped(com.twitter.mesos.gen.JobKey, Iterable)}.
   * @param role The role of the job of the shardIds.
   * @param jobName The name of the job of the shardIds.
   * @param shardIds Shard Ids of the target job.
   * @return A new Builder scoped to the given shardIds.
   */
  @Deprecated
  public static Builder shardScoped(String role, String jobName, Iterable<Integer> shardIds) {
    return unscoped().byShards(role, jobName, shardIds);
  }

  public static Builder shardScoped(JobKey jobKey, Iterable<Integer> shardIds) {
    return unscoped().byShards(jobKey, shardIds);
  }

  public static Builder taskScoped(String taskId, String... taskIds) {
    return unscoped().byId(taskId, taskIds);
  }

  public static Builder taskScoped(Iterable<String> taskIds) {
    return unscoped().byId(taskIds);
  }

  public static Builder slaveScoped(String slaveHost) {
    return unscoped().bySlave(slaveHost);
  }

  /**
   * A Builder of TaskQueries. Builders are immutable and provide access to a set of convenience
   * methods to return a new builder of another scope. Available scope filters include slave,
   * taskId, role, jobs of a role, and shards of a job.
   *
   * <p>
   * This class does not expose the full functionality of TaskQuery but rather subsets of it that
   * can be efficiently executed and make sense in the context of the scheduler datastores. This
   * builder should be preferred over constructing TaskQueries directly.
   * </p>
   *
   * TODO(ksweeney): Add an environment scope.
   */
  public static final class Builder implements Supplier<TaskQuery> {
    private final TaskQuery query;

    private Builder() {
      this.query = new TaskQuery();
    }

    private Builder(final TaskQuery query) {
      this.query = query; // It is expected that the caller calls deepCopy.
    }

    /**
     * Build a query that is the combination of all the filters applied to a Builder. Mutating the
     * returned object will not affect the state of the builder. {@link #get()} can be called any
     * number of times and will return a new {@code TaskQuery} each time.
     *
     * @return A new TaskQuery satisfying this builder's constraints.
     */
    @Override
    public TaskQuery get() {
      return query.deepCopy();
    }

    @Override
    public boolean equals(Object that) {
      return that != null
          && that instanceof Builder
          && Objects.equal(query, ((Builder) that).query);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(query);
    }

    @Override
    public String toString() {
      return Objects.toStringHelper(this)
          .add("query", query)
          .toString();
    }

    /**
     * Create a builder scoped to tasks.
     *
     * @param taskId An ID of a task to scope the builder to.
     * @param taskIds Additional IDs of tasks to scope the builder to (they are ORed together).
     * @return A new Builder scoped to the given tasks.
     */
    public Builder byId(String taskId, String... taskIds) {
      checkNotNull(taskId);

      return new Builder(
          query.deepCopy()
              .setTaskIds(ImmutableSet.<String>builder().add(taskId).add(taskIds).build()));
    }

    /**
     * Create a builder scoped to tasks.
     *
     * @see #byId(String, String...)
     *
     * @param taskIds The IDs of the tasks to scope the query to (ORed together).
     * @return A new Builder scoped to the given tasks.
     */
    public Builder byId(Iterable<String> taskIds) {
      checkNotNull(taskIds);

      return new Builder(
          query.deepCopy().setTaskIds(ImmutableSet.copyOf(taskIds)));
    }

    /**
     * Create a builder scoped to a role. A role scope conflicts with job and shards scopes.
     *
     * @param role The role to scope the query to.
     * @return A new Builder scoped to the given role.
     */
    public Builder byRole(String role) {
      checkNotNull(role);

      return new Builder(
          query.deepCopy().setOwner(new Identity().setRole(role)));
    }

    /**
     * Creates a builder scoped to the job identified by (role, name). A job scope
     * conflicts with role and shards scopes.
     *
     * @deprecated Use {@link #byJob(JobKey)} instead.
     * @param role The role of the job to scope the query to.
     * @param name The name of the job to scope the query to.
     * @return A new Builder scoped to the given job.
     */
    @Deprecated
    public Builder byJob(String role, String name) {
      checkNotNull(role);
      checkNotNull(name);

      return new Builder(
          query.deepCopy().setOwner(new Identity().setRole(role)).setJobName(name));
    }

    /**
     * Returns a new builder scoped to the job uniquely identified by the given key. A job scope
     * conflicts with role and shards scopes.
     *
     * @param jobKey The key of the job to scope the query to.
     * @return A new Builder scoped to the given role
     */
    public Builder byJob(JobKey jobKey) {
      JobKeys.assertValid(jobKey);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(jobKey.getRole()))
              .setJobName(jobKey.getName()));
    }


    /**
     * Returns a new builder scoped to the slave uniquely identified by the given slaveHost. A
     * builder can only be scoped to slaves once.
     *
     * @param slaveHost The hostname of the slave to scope the
     * @return A new Builder scoped to the given slave.
     */
    public Builder bySlave(String slaveHost) {
      checkNotNull(slaveHost);
      return new Builder(query.deepCopy().setSlaveHost(slaveHost));
    }

    /**
     * Returns a new builder scoped to the given statuses. A builder can only be scoped to statuses
     * once.
     *
     * @param status The status to scope this Builder to.
     * @param statuses Additional statuses to scope this Builder to (they are ORed together).
     * @return A new Builder scoped to the given statuses.
     */
    public Builder byStatus(ScheduleStatus status, ScheduleStatus... statuses) {
      checkNotNull(status);

      return new Builder(
          query.deepCopy().setStatuses(EnumSet.of(status, statuses)));
    }

    /**
     * Create a new Builder scoped to statuses.
     *
     * @see Builder#byStatus(ScheduleStatus, ScheduleStatus...)
     *
     * @param statuses The statuses to scope this Builder to.
     * @return A new Builder scoped to the given statuses.
     */
    public Builder byStatus(Iterable<ScheduleStatus> statuses) {
      checkNotNull(statuses);

      return new Builder(
          query.deepCopy().setStatuses(EnumSet.copyOf(ImmutableSet.copyOf(statuses))));
    }

    /**
     * Returns a new Builder scoped to the given shardIds of the given job. A builder can only
     * be scoped to a set of shards, a job, or a role once.
     *
     * @deprecated Use {@link Builder#byShards(com.twitter.mesos.gen.JobKey, int, int...)}
     * @param role The role of the job of the shardIds.
     * @param jobName The name of the job of the shardIds.
     * @param shardId A shardId of the target job.
     * @param shardIds Additional shardsIds of the target job.
     * @return A new Builder scoped to the given shardIds.
     */
    @Deprecated
    public Builder byShards(String role, String jobName, int shardId, int... shardIds) {
      checkNotNull(role);
      checkNotNull(jobName);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(role))
              .setJobName(jobName)
              .setShardIds(ImmutableSet.<Integer>builder()
                  .add(shardId)
                  .addAll(Ints.asList(shardIds))
                  .build()));
    }

    /**
     * Returns a new Builder scoped to the given shards of the given job. A builder can only
     * be scoped to a set of shards, a job, or a role once.
     *
     * @param jobKey The key identifying the job.
     * @param shardId A shardId of the target job.
     * @param shardIds Additional shardIds of the target job.
     * @return A new Builder scoped to the given shardIds.
     */
    public Builder byShards(JobKey jobKey, int shardId, int... shardIds) {
      JobKeys.assertValid(jobKey);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(jobKey.getRole()))
              .setJobName(jobKey.getName())
              .setShardIds(ImmutableSet.<Integer>builder()
                  .add(shardId)
                  .addAll(Ints.asList(shardIds))
                  .build()));
    }

    /**
     * Create a new Builder scoped to shards.
     *
     * @see Builder#byShards(String, String, int, int...)
     *
     * @deprecated Use {@link Builder#byShards(com.twitter.mesos.gen.JobKey, Iterable)}
     * @param role The role of the job of the shardIds.
     * @param jobName The name of the job of the shardIds.
     * @param shardIds  ShardIds of the target job.
     * @return A new Builder scoped to the given shardIds.
     */
    @Deprecated
    public Builder byShards(String role, String jobName, Iterable<Integer> shardIds) {
      checkNotNull(role);
      checkNotNull(jobName);
      checkNotNull(shardIds);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(role))
              .setJobName(jobName)
              .setShardIds(ImmutableSet.copyOf(shardIds)));
    }

    /**
     * Create a new Builder scoped to shards.
     *
     * @see Builder#byShards(com.twitter.mesos.gen.JobKey, int, int...)
     *
     * @param jobKey The key identifying the job.
     * @param shardIds Shards of the target job.
     * @return A new Builder scoped to the given shardIds.
     */
    public Builder byShards(JobKey jobKey, Iterable<Integer> shardIds) {
      JobKeys.assertValid(jobKey);
      checkNotNull(shardIds);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(jobKey.getRole()))
              .setJobName(jobKey.getName())
              .setShardIds(ImmutableSet.copyOf(shardIds)));
    }

    /**
     * A convenience method to scope this builder to {@link Tasks#ACTIVE_STATES}.
     *
     * @return A new Builder scoped to Tasks#ACTIVE_STATES.
     */
    public Builder active() {
      return byStatus(Tasks.ACTIVE_STATES);
    }

    /**
     * A convenience method to scope this builder to {@link Tasks#TERMINAL_STATES}.
     *
     * @return A new Builder scoped to Tasks#TERMINAL_STATES.
     */
    public Builder terminal() {
      return byStatus(Tasks.TERMINAL_STATES);
    }
  }
}
