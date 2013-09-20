/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.aurora.scheduler.base;

import java.util.EnumSet;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;

import com.twitter.aurora.gen.Identity;
import com.twitter.aurora.gen.JobKey;
import com.twitter.aurora.gen.ScheduleStatus;
import com.twitter.aurora.gen.TaskQuery;

import static com.google.common.base.Preconditions.checkNotNull;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * A utility class to construct storage queries.
 * TODO(Sathya): Add some basic unit tests for isJobScoped and isOnlyJobScoped.
 */
public final class Query {

  private Query() {
    // Utility.
  }

  /**
   * Checks whether a query is scoped to a specific job.
   * A query scoped to a job specifies a role and job name.
   *
   * @param taskQuery Query to test.
   * @return {@code true} if the query specifies at least a role and job name,
   *         otherwise {@code false}.
   */
  public static boolean isJobScoped(Builder taskQuery) {
    TaskQuery query = taskQuery.get();
    return (query.getOwner() != null)
        && !isEmpty(query.getOwner().getRole())
        && !isEmpty(query.getEnvironment())
        && !isEmpty(query.getJobName());
  }

  /**
   * Checks whether a query is strictly scoped to a specific job. A query is strictly job scoped,
   * iff it has the role, environment and jobName set.
   *
   * @param query Query to test.
   * @return {@code true} if the query is strictly job scoped, otherwise {@code false}.
   */
  public static boolean isOnlyJobScoped(Builder query) {
    Optional<JobKey> jobKey = JobKeys.from(query);
    return jobKey.isPresent() && Query.jobScoped(jobKey.get()).equals(query);
  }

  public static Builder arbitrary(TaskQuery query) {
    return new Builder(query.deepCopy());
  }

  public static Builder unscoped() {
    return new Builder();
  }

  public static Builder roleScoped(String role) {
    return unscoped().byRole(role);
  }

  public static Builder envScoped(String role, String environment) {
    return unscoped().byEnv(role, environment);
  }

  public static Builder jobScoped(JobKey jobKey) {
    return unscoped().byJob(jobKey);
  }

  public static Builder shardScoped(JobKey jobKey, int shardId, int... shardIds) {
    return unscoped().byShards(jobKey, shardId, shardIds);
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

  public static Builder statusScoped(ScheduleStatus status, ScheduleStatus... statuses) {
    return unscoped().byStatus(status, statuses);
  }

  public static Builder statusScoped(Iterable<ScheduleStatus> statuses) {
    return unscoped().byStatus(statuses);
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
      this.query = checkNotNull(query); // It is expected that the caller calls deepCopy.
    }

    /**
     * Build a query that is the combination of all the filters applied to a Builder. Mutating the
     * returned object will not affect the state of the builder. Can be called any number of times
     * and will return a new {@code TaskQuery} each time.
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
     * Create a builder scoped to an environment. An environment scope conflicts with role, job,
     * and shard scopes.
     *
     * @param role The role to scope the query to.
     * @param environment The environment to scope the query to.
     * @return A new Builder scoped to the given environment.
     */
    public Builder byEnv(String role, String environment) {
      checkNotNull(role);
      checkNotNull(environment);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(role))
              .setEnvironment(environment));
    }

    /**
     * Returns a new builder scoped to the job uniquely identified by the given key. A job scope
     * conflicts with role and shards scopes.
     *
     * @param jobKey The key of the job to scope the query to.
     * @return A new Builder scoped to the given jobKey.
     */
    public Builder byJob(JobKey jobKey) {
      JobKeys.assertValid(jobKey);

      return new Builder(
          query.deepCopy()
              .setOwner(new Identity().setRole(jobKey.getRole()))
              .setEnvironment(jobKey.getEnvironment())
              .setJobName(jobKey.getName()));
    }

    /**
     * Returns a new builder scoped to the slave uniquely identified by the given slaveHost. A
     * builder can only be scoped to slaves once.
     *
     * @param slaveHost The hostname of the slave to scope the query to.
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
              .setEnvironment(jobKey.getEnvironment())
              .setJobName(jobKey.getName())
              .setShardIds(ImmutableSet.<Integer>builder()
                  .add(shardId)
                  .addAll(Ints.asList(shardIds))
                  .build()));
    }

    /**
     * Create a new Builder scoped to shards.
     *
     * @see Builder#byShards(com.twitter.aurora.gen.JobKey, int, int...)
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
              .setEnvironment(jobKey.getEnvironment())
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
