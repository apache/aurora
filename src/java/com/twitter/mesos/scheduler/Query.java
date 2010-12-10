package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.TaskStore.TaskState;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Comparator;
import java.util.Set;
import java.util.SortedSet;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * A query that can be used to find tasks in the task store.
 *
 * @author wfarner
 */
public class Query implements Predicate<TaskState> {
  private final TaskQuery baseQuery;
  private final Predicate<TaskState> filter;
  private final Predicate<TaskState> fullFilter;

  /**
   * Creates a new query with the given base query and optional filters.
   *
   * @param baseQuery Base query.
   * @param filters Filters to apply.
   */
  public Query(TaskQuery baseQuery, Predicate<TaskState>... filters) {
    Preconditions.checkNotNull(baseQuery);

    this.baseQuery = Preconditions.checkNotNull(baseQuery);
    if (filters.length == 0) {
      this.filter = null;
      this.fullFilter = taskMatcher(baseQuery);
    } else {
      this.filter = Predicates.and(filters);
      this.fullFilter = Predicates.and(taskMatcher(baseQuery), filter);
    }
  }

  /**
   * Creates a new query that adds a filter to an existing query.
   *
   * @param query Query to add a filter to, which will not be modified.
   * @param filter Filter to add to {@code query}.
   * @return A new query with an additional filter.
   */
  public static Query and(Query query, Predicate<TaskState> filter) {
    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(filter);
    return new Query(query.base(), query.filter == null
        ? filter : Predicates.<TaskState>and(query.filter, filter));
  }

  /**
   * Creates a new query that composes an existing query with another task query.
   *
   * @param query Query to compose with another query.
   * @param additionalQuery Query to add.
   * @return A new query that will match the intersection of the provided queries.
   */
  public static Query and(Query query, TaskQuery additionalQuery) {
    return and(query, taskMatcher(additionalQuery));
  }

  @Override
  public boolean apply(TaskState task) {
    return fullFilter.apply(task);
  }

  /**
   * Gets the base query.
   *
   * @return The base query, which may be modified.
   */
  public TaskQuery base() {
    return baseQuery;
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
    return specifiesJob() && (filter == null)
           && (base().getStatusesSize() == 0) && (base().getTaskIdsSize() == 0);
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

  public static Query GET_ALL = new Query(new TaskQuery());

  public static Query byId(Iterable<Integer> taskIds) {
    return new Query(new TaskQuery().setTaskIds(ImmutableSet.copyOf(taskIds)));
  }

  public static Query byId(int taskId) {
    return byId(ImmutableSet.of(taskId));
  }

  public static Query liveShard(String jobKey, int shard) {
    return new Query(new TaskQuery().setJobKey(jobKey).setShardIds(ImmutableSet.of(shard)),
        Tasks.ACTIVE_FILTER);
  }

  public static Query byStatus(ScheduleStatus... statuses) {
    return new Query(new TaskQuery().setStatuses(ImmutableSet.copyOf(statuses)));
  }

  public static Query byStatus(Set<ScheduleStatus> statuses) {
    return new Query(new TaskQuery().setStatuses(ImmutableSet.copyOf(statuses)));
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

  /**
   * Returns a predicate that will match tasks against the given {@code query}.
   *
   * @param query The query to use for finding tasks.
   * @return A predicate that will match tasks meeting the criteria in the query.
   */
  private static Predicate<TaskState> taskMatcher(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    return new Predicate<TaskState>() {
      private boolean matches(String query, String value) {
        return StringUtils.isEmpty(query) || (value != null && value.matches(query));
      }

      private <T> boolean matches(Collection<T> collection, T item) {
        return collection == null || collection.contains(item);
      }

      @Override public boolean apply(TaskState state) {
        AssignedTask assigned = Preconditions.checkNotNull(state.task.getAssignedTask());
        TwitterTaskInfo t = Preconditions.checkNotNull(assigned.getTask());
        return matches(query.getOwner(), t.getOwner())
            && matches(query.getJobName(), t.getJobName())
            && matches(query.getJobKey(), Tasks.jobKey(t))
            && matches(query.getTaskIds(), assigned.getTaskId())
            && matches(query.getShardIds(), assigned.getTask().getShardId())
            && matches(query.getStatuses(), state.task.getStatus())
            // TODO(wfarner): Might have to be smarter here so as to not be burned by different
            //    host names for the same machine. i.e. machine1, machine1.prod.twitter.com
            && matches(query.getSlaveHost(), assigned.getSlaveHost());
      }
    };
  }

  public static SortedSet<TaskState> sortTasks(Iterable<TaskState> tasks,
      Comparator<TaskState> comparator) {
    return ImmutableSortedSet.copyOf(comparator, tasks);
  }

  public static final Comparator<TaskState> SORT_BY_TASK_ID = new Comparator<TaskState>() {
    @Override public int compare(TaskState stateA, TaskState stateB) {
      return stateA.task.getAssignedTask().getTaskId() - stateB.task.getAssignedTask().getTaskId();
    }
  };

  public static final Comparator<TaskState> SORT_BY_PRIORITY = new Comparator<TaskState>() {
    @Override public int compare(TaskState stateA, TaskState stateB) {
      return stateA.task.getAssignedTask().getTask().getPriority()
             - stateB.task.getAssignedTask().getTask().getPriority();
    }
  };
}
