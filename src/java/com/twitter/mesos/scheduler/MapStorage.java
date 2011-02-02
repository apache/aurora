package com.twitter.mesos.scheduler;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Command;
import com.twitter.common.stats.RequestStats;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.gen.AssignedTask;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.NonVolatileSchedulerState;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.TwitterTaskInfo;
import com.twitter.mesos.scheduler.persistence.PersistenceLayer;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

/**
 * Stores all tasks configured with the scheduler.
 *
 * @author wfarner
 */
public class MapStorage implements Storage {
  private final static Logger LOG = Logger.getLogger(MapStorage.class.getName());

  // The mesos framework ID of the scheduler, set to null until the framework is registered.
  private final AtomicReference<String> frameworkId = new AtomicReference<String>(null);

  // Maps tasks by their task IDs.
  private final Map<String, ScheduledTask> tasks = Maps.newHashMap();

  private final PersistenceLayer<NonVolatileSchedulerState> persistenceLayer;

  // Schedulers that are responsible for triggering execution of jobs.
  private final ImmutableMap<String, JobManager> jobManagers;

  @Inject
  public MapStorage(PersistenceLayer<NonVolatileSchedulerState> persistenceLayer,
      Set<JobManager> jobManagers) {

    this.persistenceLayer = Preconditions.checkNotNull(persistenceLayer);
    this.jobManagers = Maps.uniqueIndex(jobManagers, new Function<JobManager, String>() {
      @Override public String apply(JobManager jobManager) {
        return jobManager.getUniqueKey();
      }
    });

    restore();
  }

  // Tracks whether the current transaction needs to persist to disk
  private final AtomicBoolean persist = new AtomicBoolean(false);

  // Tracks whether we're in the root transaction or a nested one
  private final AtomicBoolean rootTransaction = new AtomicBoolean(true);

  @Override
  public synchronized <T, E extends Exception> T doInTransaction(Work<T, E> work) throws E {
    boolean inRootTransaction = rootTransaction.getAndSet(false);
    try {
      final MapStorage self = this;

      SchedulerStore schedulerStore = new SchedulerStore() {
        @Override public void saveFrameworkId(String frameworkId) {
          self.saveFrameworkId(frameworkId);
          persist.set(true);
        }
        @Override public String fetchFrameworkId() {
          return self.fetchFrameworkId();
        }
      };

      JobStore jobStore = new JobStore() {
        @Override public Iterable<JobConfiguration> fetchJobs(String managerId) {
          return jobManagers.get(managerId).getJobs();
        }

        @Override public void saveAcceptedJob(String managerId, JobConfiguration jobConfig) {
          persist.set(true);
        }
        @Override public void deleteJob(String jobKey) {
          persist.set(true);
        }
      };

      TaskStore taskStore = asTaskStore(new Command() {
        @Override public void execute() {
          persist.set(true);
        }
      });

      T result = work.apply(schedulerStore, jobStore, taskStore);
      if (inRootTransaction && persist.get()) {
        persist();
      }
      return result;
    } finally {
      if (inRootTransaction) {
        persist.set(false);
        rootTransaction.set(true);
      }
    }
  }

  @VisibleForTesting
  TaskStore asTaskStore(final Command persist) {
    final MapStorage self = this;
    return new TaskStore() {
      @Override public void add(Set<ScheduledTask> newTasks) throws IllegalStateException {
        self.add(newTasks);
        persist.execute();
      }
      @Override public void remove(Query query) {
        if (!self.remove(query).isEmpty()) {
          persist.execute();
        }
      }
      @Override public void remove(Set<String> taskIds) {
        if (!self.remove(taskIds).isEmpty()) {
          persist.execute();
        }
      }
      @Override public ImmutableSet<ScheduledTask> mutate(Query query,
          Closure<ScheduledTask> mutator) {

        ImmutableSet<ScheduledTask> mutated = self.mutate(query, mutator);
        if (!mutated.isEmpty()) {
          persist.execute();
        }
        return mutated;
      }
      @Override public ImmutableSortedSet<ScheduledTask> fetch(Query query) {
        return self.fetch(query);
      }
      @Override public Set<String> fetchIds(Query query) {
        return self.fetchIds(query);
      }
    };
  }

  void add(Set<ScheduledTask> newTasks) throws IllegalStateException {
    Preconditions.checkState(!Iterables.any(newTasks,
        Predicates.compose(hasTaskId, Tasks.SCHEDULED_TO_ID)),
        "Proposed new tasks would create task ID collision.");
    Preconditions.checkState(
        Sets.newHashSet(transform(newTasks, Tasks.SCHEDULED_TO_ID)).size() == newTasks.size(),
        "Proposed new tasks would create task ID collision.");

    // Do a first pass to make sure all of the values are good.
    for (ScheduledTask task : newTasks) {
      Preconditions.checkNotNull(task.getAssignedTask(), "Assigned task may not be null.");
      Preconditions.checkNotNull(task.getAssignedTask().getTask(), "Task info may not be null.");
    }

    vars.tasksAdded.addAndGet(newTasks.size());

    for (ScheduledTask task : newTasks) {
      tasks.put(task.getAssignedTask().getTaskId(), task.deepCopy());
    }
  }

  Set<String> remove(Query query) {
    Set<String> removedIds = ImmutableSet.copyOf(transform(query(query), Tasks.SCHEDULED_TO_ID));

    LOG.info("Removing tasks " + removedIds);
    vars.tasksRemoved.addAndGet(removedIds.size());
    tasks.keySet().removeAll(removedIds);
    return removedIds;
  }

  Set<String> remove(Set<String> taskIds) {
    if (taskIds.isEmpty()) {
      return ImmutableSet.of();
    }

    return remove(Query.byId(taskIds));
  }

  ImmutableSet<ScheduledTask> mutate(Query query, Closure<ScheduledTask> mutator) {
    Iterable<ScheduledTask> mutables = mutableQuery(query);
    for (ScheduledTask mutable : mutables) {
      mutator.execute(mutable);
    }
    return ImmutableSet.copyOf(transform(mutables, STATE_COPY));
  }

  ImmutableSortedSet<ScheduledTask> fetch(Query query) {
    return Query.sortTasks(query(query), Query.SORT_BY_TASK_ID);
  }

  Set<String> fetchIds(Query query) {
    return ImmutableSet.copyOf(Iterables.transform(fetch(query), Tasks.SCHEDULED_TO_ID));
  }

  // TODO(jsirois): XXX verify reads are behind barriers
  private boolean stopped = false;

  @Override
  public synchronized void stop() {
    stopped = true;
  }

  void saveFrameworkId(String frameworkId) {
    this.frameworkId.set(checkNotNull(frameworkId));
  }

  String fetchFrameworkId() {
    return frameworkId.get();
  }

  private void persist() {
    if (stopped) {
      LOG.severe("Scheduler was stopped, ignoring persist request.");
      return;
    }

    LOG.info("Saving scheduler state.");
    NonVolatileSchedulerState state = new NonVolatileSchedulerState()
        .setFrameworkId(frameworkId.get())
        .setTasks(ImmutableList.copyOf(fetch(Query.GET_ALL)));
    Map<String, List<JobConfiguration>> moduleState = Maps.newHashMap();
    for (Entry<String, JobManager> entry : jobManagers.entrySet()) {
      moduleState.put(entry.getKey(), Lists.newArrayList(entry.getValue().getJobs()));
    }
    state.setModuleJobs(moduleState);

    try {
      long startNanos = System.nanoTime();
      persistenceLayer.commit(state);
      vars.persistLatency.requestComplete((System.nanoTime() - startNanos) / 1000);
    } catch (PersistenceLayer.PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to persist scheduler state.", e);
    }
  }

  private void restore() {
    LOG.info("Attempting to recover persisted state.");

    NonVolatileSchedulerState state;
    try {
      state = persistenceLayer.fetch();
      if (state == null) {
        LOG.info("No persisted state found for restoration.");
        return;
      }
    } catch (PersistenceLayer.PersistenceException e) {
      LOG.log(Level.SEVERE, "Failed to fetch persisted state.", e);
      return;
    }

    frameworkId.set(state.getFrameworkId());
    add(ImmutableSet.copyOf(state.getTasks()));
  }

  /**
   * Performs a query over the current task state, where modifications to the results will not
   * be reflected in the store.
   *
   * @param query The query to execute.
   * @return A copy of all the task states matching the query.
   */
  private ImmutableSet<ScheduledTask> query(Query query) {
    // Copy before filtering, so that client code does not access mutable state.
    vars.queries.incrementAndGet();
    ImmutableSet<ScheduledTask> results = ImmutableSet.copyOf(filterQuery(
        transform(getIntermediateResults(query.base()), STATE_COPY), query));
    vars.queryResults.incrementAndGet();
    return results;
  }

  /**
   * Performs a query over the current task state, where the results are mutable.
   *
   * @param query The query to execute.
   * @return A copy of all the task states matching the query.
   */
  private ImmutableSet<ScheduledTask> mutableQuery(Query query) {
    vars.mutableQueries.incrementAndGet();
    ImmutableSet<ScheduledTask> results =
        ImmutableSet.copyOf(filterQuery(getIntermediateResults(query.base()), query));
    vars.mutableResults.addAndGet(results.size());
    return results;
  }

  private static Iterable<ScheduledTask> filterQuery(Iterable<ScheduledTask> tasks, Query query) {
    return filter(tasks, Predicates.and(taskMatcher(query.base()), query.postFilter()));
  }

  /**
   * Gets the intermediate (pre-filtered) results for a query by using task IDs if specified.
   *
   * @param query The query being performed.
   * @return Intermediate results for the query.
   */
  private Iterable<ScheduledTask> getIntermediateResults(TaskQuery query) {
    if (query.getTaskIdsSize() > 0) {
      return getStateById(query.getTaskIds());
    } else {
      vars.fullScanQueries.incrementAndGet();
      return tasks.values();
    }
  }

  private final Predicate<String> hasTaskId = new Predicate<String>() {
    @Override public boolean apply(String taskId) {
      return tasks.containsKey(taskId);
    }
  };

  private final Function<String, ScheduledTask> getById = new Function<String, ScheduledTask>() {
    @Override public ScheduledTask apply(String taskId) {
      return tasks.get(taskId);
    }
  };

  private static final Function<ScheduledTask, ScheduledTask> STATE_COPY =
      new Function<ScheduledTask, ScheduledTask>() {
        @Override public ScheduledTask apply(ScheduledTask task) {
          return new ScheduledTask(task);
        }
      };

  /**
   * Gets task states by ID, ommitting tasks not found.
   *
   * @param taskIds IDs of tasks to look up.
   * @return Tasks found that match the given task IDs.
   */
  private Set<ScheduledTask> getStateById(Set<String> taskIds) {
    return ImmutableSet.copyOf(Iterables.filter(Iterables.transform(taskIds, getById),
        Predicates.notNull()));
  }

  /**
   * Returns a predicate that will match tasks against the given {@code query}.
   *
   * @param query The query to use for finding tasks.
   * @return A predicate that will match tasks meeting the criteria in the query.
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
            && matches(query.getJobKey(), Tasks.jobKey(t))
            && matches(query.getTaskIds(), assigned.getTaskId())
            && matches(query.getShardIds(), assigned.getTask().getShardId())
            && matches(query.getStatuses(), task.getStatus())
            // TODO(wfarner): Might have to be smarter here so as to not be burned by different
            //    host names for the same machine. i.e. machine1, machine1.prod.twitter.com
            && matches(query.getSlaveHost(), assigned.getSlaveHost());
      }
    };
  }

  private class Vars {
    private final RequestStats persistLatency = new RequestStats("scheduler_persist");
    private final AtomicLong queries = Stats.exportLong("task_store_queries");
    private final AtomicLong queryResults = Stats.exportLong("task_store_query_results");
    private final AtomicLong mutableQueries = Stats.exportLong("task_store_mutable_queries");
    private final AtomicLong mutableResults = Stats.exportLong("task_store_mutable_query_results");
    private final AtomicLong fullScanQueries = Stats.exportLong("task_store_full_scan_queries");
    private final AtomicLong tasksAdded = Stats.exportLong("task_store_tasks_added");
    private final AtomicLong tasksRemoved = Stats.exportLong("task_store_tasks_removed");

    Vars() {
      Stats.export(new StatImpl<Integer>("task_store_size") {
          @Override public Integer read() { return tasks.size(); }
      });
    }
  }
  private final Vars vars = new Vars();
}
