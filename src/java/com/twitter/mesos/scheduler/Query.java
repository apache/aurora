package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.SchedulerCore.TaskState;

import java.util.Comparator;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * A query that can be used to find tasks in the task store.
 *
 * @author wfarner
 */
public class Query {
  private static final Predicate<ScheduledTask> NO_POST_FILTER = Predicates.alwaysTrue();

  private final TaskQuery baseQuery;
  private final Predicate<ScheduledTask> filter;

  /**
   * Creates a new query with the given base query and optional filters.
   *
   * @param baseQuery Base query.
   * @param filters Filters to apply.
   */
  public Query(TaskQuery baseQuery, Predicate<ScheduledTask>... filters) {
    Preconditions.checkNotNull(baseQuery);

    this.baseQuery = Preconditions.checkNotNull(baseQuery);
    if (filters.length == 0) {
      this.filter = NO_POST_FILTER;
    } else {
      this.filter = Predicates.and(filters);
    }
  }

  /**
   * Creates a new query that adds a filter to an existing query.
   *
   * @param query Query to add a filter to, which will not be modified.
   * @param filter Filter to add to {@code query}.
   * @return A new query with an additional filter.
   */
  public static Query and(Query query, Predicate<ScheduledTask> filter) {
    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(filter);
    return new Query(query.base(), Predicates.<ScheduledTask>and(query.filter, filter));
  }

  /**
   * Gets the base query.
   *
   * @return The base query, which may be modified.
   */
  public TaskQuery base() {
    return baseQuery;
  }

  public Predicate<ScheduledTask> postFilter() {
    return filter;
  }

  /**
   * Checks whether this query specifies a job by key or owner and name.
   *
   * @return {@code true} if this query will filter to a specific job, {@code false} otherwise.
   */
  public boolean specifiesJob() {
    return (!isEmpty(base().getOwner()) && !isEmpty(base().getJobName())
            || !isEmpty(base().getJobKey()));
  }

  /**
   * Determines whether this query is only filtering by job.
   *
   * @return {@code true} If the only filtering in this query is by job, {@code false} otherwise.
   */
  public boolean specifiesJobOnly() {
    return specifiesJob() && !hasPostFilter()
           && (base().getStatusesSize() == 0) && (base().getTaskIdsSize() == 0);
  }

  /**
   * Determines whether this query relies on a post filter predicate in addition to its
   * {@link TaskQuery}.
   *
   * @return {@code true} if the query uses a post filter predicate
   */
  public boolean hasPostFilter() {
    return filter != NO_POST_FILTER;
  }

  /**
   * Gets the job key that this query specifies.
   *
   * @return The job key (explicit or by owner/name) for this query, or {@code null} if the query
   *    does not specify a job key.
   */
  public String getJobKey() {
    if (!specifiesJob()) return null;

    return isEmpty(base().getJobKey())
          ? Tasks.jobKey(base().getOwner(), base().getJobName()) : base().getJobKey();
  }

  @Override
  public String toString() {
    String result = baseQuery.toString();
    if (filter != null) result = "Base: " + result + ", filter " + filter;
    return result;
  }

  public static final Query GET_ALL = new Query(new TaskQuery());

  public static Query byId(Iterable<String> taskIds) {
    return new Query(new TaskQuery().setTaskIds(ImmutableSet.copyOf(taskIds)));
  }

  public static Query byId(String taskId) {
    return byId(ImmutableSet.of(taskId));
  }

  public static Query liveShard(String jobKey, int shard) {
    return new Query(new TaskQuery().setJobKey(jobKey).setShardIds(ImmutableSet.of(shard)),
        Tasks.ACTIVE_FILTER);
  }

  public static Query byStatus(ScheduleStatus status) {
    return new Query(new TaskQuery().setStatuses(ImmutableSet.of(status)));
  }

  public static Query activeQuery(String jobKey) {
    return new Query(new TaskQuery().setJobKey(jobKey).setStatuses(Tasks.ACTIVE_STATES));
  }

  public static Query activeQuery(String owner, String jobName) {
    return activeQuery(Tasks.jobKey(owner, jobName));
  }

  public static Query activeQuery(JobConfiguration job) {
    return activeQuery(job.getOwner(), job.getName());
  }

  public static ImmutableSortedSet<ScheduledTask> sortTasks(Iterable<ScheduledTask> tasks,
      Comparator<ScheduledTask> comparator) {
    return ImmutableSortedSet.copyOf(comparator, tasks);
  }

  public static final Comparator<ScheduledTask> SORT_BY_TASK_ID = new Comparator<ScheduledTask>() {
    @Override public int compare(ScheduledTask taskA, ScheduledTask taskB) {
      return taskA.getAssignedTask().getTaskId().compareTo(taskB.getAssignedTask().getTaskId());
    }
  };
}
