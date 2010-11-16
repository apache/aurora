package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.LiveTask;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

/**
 * Stores all tasks configured with the scheduler.
 *
 * TODO(wfarner): Make this the owner of SchedulerState, and persistence.
 *
 * @author wfarner
 */
public class TaskStore {
  private final static Logger LOG = Logger.getLogger(TaskStore.class.getName());

  private final List<ScheduledTask> tasks = Collections.synchronizedList(
      Lists.<ScheduledTask>newLinkedList());
  private final Map<Integer, VolatileTaskState> volatileTaskStates = new MapMaker()
      .makeComputingMap(new Function<Integer, VolatileTaskState>() {
        @Override public VolatileTaskState apply(Integer taskId) {
          return new VolatileTaskState(taskId);
        }
      });

  /**
   * Adds tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in the
   * state they were in when the method is called, and further object modifications will not affect
   * the tasks.
   *
   * @param newTasks Tasks to add.
   */
  public void add(Iterable<ScheduledTask> newTasks) {
    // Do a sanity check and make sure we're not adding a task with a duplicate task id.
    Set<Integer> newTaskIds = Sets.newHashSet(Iterables.transform(newTasks, Tasks.GET_TASK_ID));
    Preconditions.checkArgument(newTaskIds.size() == Iterables.size(newTasks),
        "Duplicate task IDs not allowed: " + newTasks);

    for (ScheduledTask task : tasks) {
      Preconditions.checkNotNull(task.getAssignedTask());
      Preconditions.checkNotNull(task.getAssignedTask().getTask());
      Preconditions.checkArgument(!newTaskIds.contains(task.getAssignedTask().getTaskId()));
    }

    tasks.addAll(Lists.newArrayList(Iterables.transform(newTasks,
        new Function<ScheduledTask, ScheduledTask>() {
      @Override public ScheduledTask apply(ScheduledTask task) {
        return new ScheduledTask(task);
      }
    })));
  }

  /**
   * Removes tasks from the store.
   *
   * @param removedTasks Tasks to remove.
   */
  public void remove(Iterable<ScheduledTask> removedTasks) {
    if (Iterables.isEmpty(removedTasks)) return;

    Set<Integer> removedIds = Sets.newHashSet(Iterables.transform(removedTasks, Tasks.GET_TASK_ID));
    LOG.info("Removing tasks " + removedIds);

    int sizeBefore = tasks.size();
    tasks.removeAll(Lists.newArrayList(removedTasks));
    volatileTaskStates.keySet().removeAll(removedIds);
    int removed = sizeBefore - tasks.size();
    if (removed > 0) LOG.info(String.format("Removed %d tasks from task store.", removed));
  }

  /**
   * Offers temporary mutable access to tasks.
   *
   * @param immutableCopies The immutable copies of tasks to mutate.
   * @param mutator The mutate operation.
   * @param <E> Type of exception that the mutator may throw.
   * @return Immutable copies of the mutated tasks.
   * @throws E An exception, specified by the mutator.
   */
  public <E extends Exception> Iterable<ScheduledTask> mutate(
      Iterable<ScheduledTask> immutableCopies, final ExceptionalClosure<ScheduledTask, E> mutator)
      throws E {
    List<ScheduledTask> copies = Lists.newArrayList();
    for (ScheduledTask task : immutableCopies) {
      // TODO(wfarner): This would be faster with something equivalent to an identity set.
      ScheduledTask mutable = tasks.get(tasks.indexOf(task));
      mutator.execute(mutable);
      copies.add(new ScheduledTask(mutable));
    }

    return copies;
  }

  /**
   * Convenience function to mutate all results from a task query.
   *
   * @param query Query for tasks to mutate.
   * @param mutator Mutate operation.
   * @param <E> Type of exception that the mutator may throw.
   * @return Immutable copies of the mutated tasks.
   * @throws E An exception, specified by the mutator.
   */
  public <E extends Exception> Iterable<ScheduledTask> mutate(TaskQuery query,
      final ExceptionalClosure<ScheduledTask, E> mutator) throws E {
    return mutate(fetch(query), mutator);
  }

  /**
   * Offers temporary mutable access to a task.
   *
   * @param immutableCopy The immutable copy to mutate.
   * @param mutator The mutate operation.
   * @param <E> Type of exception that the mutator may throw.
   * @return An immutable copy of the mutated task.
   * @throws E An exception, specified by the mutator.
   */
  public <E extends Exception> ScheduledTask mutate(ScheduledTask immutableCopy,
      final ExceptionalClosure<ScheduledTask, E> mutator) throws E {
    return Iterables.get(mutate(Lists.newArrayList(immutableCopy), mutator), 0);
  }

  /**
   * Fetches a read-only view of tasks matching a query and filters.
   *
   * @param query Query to identify tasks with.
   * @param filters Additional filters to apply.
   * @return A read-only view of matching tasks.
   */
  public Set<ScheduledTask> fetch(TaskQuery query, Predicate<ScheduledTask>... filters) {
    return Sets.newHashSet(Iterables.filter(snapshot(), makeFilter(query, filters)));
  }

  private final Function<ScheduledTask, LiveTask> getLiveTask =
      new Function<ScheduledTask, LiveTask>() {
          @Override public LiveTask apply(ScheduledTask task) {
            VolatileTaskState volatileState = volatileTaskStates.get(
                task.getAssignedTask().getTaskId());
            return new LiveTask(task, volatileState.resources);
          }
        };

  public Iterable<LiveTask> getLiveTasks(Iterable<ScheduledTask> scheduledTasks) {
    return Iterables.transform(scheduledTasks, getLiveTask);
  }

  public void mutateVolatileState(int taskId, Closure<VolatileTaskState> mutator) {
    mutator.execute(volatileTaskStates.get(taskId));
  }

  /**
   * Returns a predicate that will match tasks against the given {@code query}.
   *
   * @param query The query to use for finding tasks.
   * @return An iterable containing all matching tasks
   */
  private static Predicate<ScheduledTask> taskMatcher(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    return new Predicate<ScheduledTask>() {
      private boolean matches(String query, String value) {
        return StringUtils.isEmpty(query) || (value != null && value.matches(query));
      }

      private <T> boolean matches(Collection<T> collection, T item) {
        return collection == null || collection.contains(item);
      }

      @Override public boolean apply(ScheduledTask task) {
        AssignedTask assigned = Preconditions.checkNotNull(task.getAssignedTask());
        TwitterTaskInfo t = Preconditions.checkNotNull(assigned.getTask());
        return matches(query.getOwner(), t.getOwner())
            && matches(query.getJobName(), t.getJobName())
            && matches(query.getTaskIds(), assigned.getTaskId())
            && matches(query.getStatuses(), task.getStatus())
            // TODO(wfarner): Might have to be smarter here so as to not be burned by different
            //    host names for the same machine. i.e. machine1, machine1.prod.twitter.com
            && matches(query.getSlaveHost(), assigned.getSlaveHost());
      }
    };
  }

  private static Predicate<ScheduledTask> makeFilter(TaskQuery query,
      Predicate<ScheduledTask>... filters) {
    Predicate<ScheduledTask> filter = taskMatcher(Preconditions.checkNotNull(query));
    if (filters.length > 0) {
      filter = Predicates.and(filter, Predicates.and(filters));
    }

    return filter;
  }

  private Iterable<ScheduledTask> snapshot() {
    synchronized(tasks) {
      return Lists.newLinkedList(Iterables.transform(tasks, new Function<ScheduledTask, ScheduledTask>() {
        @Override public ScheduledTask apply(ScheduledTask task) {
          return new ScheduledTask(task);
        }
      }));
    }
  }

  // TODO(wfarner): Use this comparator to keep tasks sorted by priority.
  private static final Comparator<ScheduledTask> PRIORITY_COMPARATOR =
      new Comparator<ScheduledTask>() {
    @Override public int compare(ScheduledTask taskA, ScheduledTask taskB) {
      return taskA.getAssignedTask().getTask().getPriority()
             - taskB.getAssignedTask().getTask().getPriority();
    }
  };
}
