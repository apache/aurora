package com.twitter.nexus.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.nexus.gen.TaskQuery;
import com.twitter.nexus.gen.TrackedTask;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.List;

/**
 * Stores all tasks configured with the scheduler.
 *
 * TODO(wfarner): Make this the owner of SchedulerState, and persistence.
 *
 * @author wfarner
 */
public class TaskStore {
  private final List<TrackedTask> tasks = Lists.newArrayList();

  /**
   * Adds tasks to the store.  Tasks are copied internally, meaning that the tasks are stored in the
   * state they were in when the method is called, and further object modifications will not affect
   * the tasks.
   *
   * @param newTasks Tasks to add.
   */
  public void add(Iterable<TrackedTask> newTasks) {
    tasks.addAll(Lists.newArrayList(Iterables.transform(newTasks,
        new Function<TrackedTask, TrackedTask>() {
      @Override public TrackedTask apply(TrackedTask task) {
        return new TrackedTask(task);
      }
    })));
  }

  /**
   * Removes tasks from the store.
   *
   * @param removedTasks Tasks to remove.
   */
  public void remove(Iterable<TrackedTask> removedTasks) {
    tasks.removeAll(Lists.newArrayList(removedTasks));
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
  public <E extends Exception> Iterable<TrackedTask> mutate(Iterable<TrackedTask> immutableCopies,
      final ExceptionalClosure<TrackedTask, E> mutator) throws E {
    List<TrackedTask> copies = Lists.newArrayList();
    for (TrackedTask task : immutableCopies) {
      TrackedTask mutable = tasks.get(tasks.indexOf(task));
      mutator.execute(mutable);
      copies.add(new TrackedTask(mutable));
    }

    return copies;
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
  public <E extends Exception> TrackedTask mutate(TrackedTask immutableCopy,
      final ExceptionalClosure<TrackedTask, E> mutator) throws E {
    return Iterables.get(mutate(Lists.newArrayList(immutableCopy), mutator), 0);
  }

  /**
   * Fetches a read-only view of tasks matching a query and filters.
   *
   * @param query Query to identify tasks with.
   * @param filters Additional filters to apply.
   * @return A read-only view of matching tasks.
   */
  public Iterable<TrackedTask> fetch(TaskQuery query, Predicate<TrackedTask>... filters) {
    return Iterables.filter(snapshot(), makeFilter(query, filters));
  }

  /**
   * Returns a predicate that will match tasks against the given {@code query}.
   *
   * @param query The query to use for finding tasks.
   * @return An iterable containing all matching tasks
   */
  public static Predicate<TrackedTask> taskMatcher(final TaskQuery query) {
    Preconditions.checkNotNull(query);
    return new Predicate<TrackedTask>() {
      private boolean matches(String query, String value) {
        return StringUtils.isEmpty(query) || value.matches(query);
      }

      private <T> boolean matches(Collection<T> collection, T item) {
        return collection == null || collection.isEmpty() || collection.contains(item);
      }

      @Override public boolean apply(TrackedTask task) {
        return matches(query.getOwner(), task.getOwner())
            && matches(query.getJobName(), task.getJobName())
            && matches(query.getTaskIds(), task.getTaskId())
            && matches(query.getStatuses(), task.getStatus());
      }
    };
  }

  private Predicate<TrackedTask> makeFilter(TaskQuery query, Predicate<TrackedTask>... filters) {
    Predicate<TrackedTask> filter = taskMatcher(Preconditions.checkNotNull(query));
    if (filters.length > 0) {
      filter = Predicates.and(filter, Predicates.and(filters));
    }

    return filter;
  }

  private Iterable<TrackedTask> snapshot() {
    return Iterables.transform(tasks, new Function<TrackedTask, TrackedTask>() {
      @Override public TrackedTask apply(TrackedTask task) {
        return new TrackedTask(task);
      }
    });
  }
}
