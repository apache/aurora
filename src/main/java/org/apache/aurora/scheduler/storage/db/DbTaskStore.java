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
package org.apache.aurora.scheduler.storage.db;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.util.Clock;

import org.apache.aurora.gen.Constraint;
import org.apache.aurora.gen.Container;
import org.apache.aurora.gen.ScheduledTask;
import org.apache.aurora.gen.TaskConfig;
import org.apache.aurora.gen.TaskConstraint;
import org.apache.aurora.scheduler.base.Query;
import org.apache.aurora.scheduler.base.Query.Builder;
import org.apache.aurora.scheduler.base.Tasks;
import org.apache.aurora.scheduler.storage.TaskStore;
import org.apache.aurora.scheduler.storage.db.views.AssignedPort;
import org.apache.aurora.scheduler.storage.db.views.ScheduledTaskWrapper;
import org.apache.aurora.scheduler.storage.db.views.TaskConfigRow;
import org.apache.aurora.scheduler.storage.entities.IJobKey;
import org.apache.aurora.scheduler.storage.entities.IScheduledTask;
import org.apache.aurora.scheduler.storage.entities.ITaskConfig;

import static java.util.Objects.requireNonNull;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A task store implementation based on a relational database.
 * <p>
 * TODO(wfarner): Consider modifying code generator to support directly producing ITaskConfig, etc
 * from myBatis (it will set private final fields just fine).  This would reduce memory and time
 * spent translating and copying objects.
 */
class DbTaskStore implements TaskStore.Mutable {

  private static final Logger LOG = Logger.getLogger(DbTaskStore.class.getName());

  private final TaskMapper taskMapper;
  private final TaskConfigManager configManager;
  private final JobKeyMapper jobKeyMapper;
  private final Clock clock;
  private final long slowQueryThresholdNanos;

  @Inject
  DbTaskStore(
      TaskMapper taskMapper,
      TaskConfigManager configManager,
      JobKeyMapper jobKeyMapper,
      Clock clock,
      Amount<Long, Time> slowQueryThreshold) {

    LOG.warning("DbTaskStore is experimental, and should not be used in production clusters!");
    this.taskMapper = requireNonNull(taskMapper);
    this.configManager = requireNonNull(configManager);
    this.jobKeyMapper = requireNonNull(jobKeyMapper);
    this.clock = requireNonNull(clock);
    this.slowQueryThresholdNanos =  slowQueryThreshold.as(Time.NANOSECONDS);
  }

  @Timed("db_storage_fetch_tasks")
  @Override
  public ImmutableSet<IScheduledTask> fetchTasks(Builder query) {
    requireNonNull(query);

    // TODO(wfarner): Consider making slow query logging more reusable, or pushing it down into the
    //                database.
    long start = clock.nowNanos();
    ImmutableSet<IScheduledTask> result = matches(query).toSet();
    long durationNanos = clock.nowNanos() - start;
    Level level = durationNanos >= slowQueryThresholdNanos ? Level.INFO : Level.FINE;
    if (LOG.isLoggable(level)) {
      Long time = Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS);
      LOG.log(level, "Query took " + time + " ms: " + query.get());
    }

    return result;
  }

  @Timed("db_storage_get_job_keys")
  @Override
  public ImmutableSet<IJobKey> getJobKeys() {
    return IJobKey.setFromBuilders(taskMapper.selectJobKeys());
  }

  private static final Function<TaskConfigRow, Long> CONFIG_ID =
      new Function<TaskConfigRow, Long>() {
        @Override
        public Long apply(TaskConfigRow row) {
          return row.getId();
        }
      };

  /**
   * Computes an association between config table row ID and {@link ITaskConfig} object for all
   * configs in the provided jobs.
   *
   * @param jobs Jobs to fetch task configs for.
   * @return A mutable bi-map between row ID and task config.
   */
  private Map<ITaskConfig, Long> getTaskConfigRows(Set<IJobKey> jobs) {
    Function<IJobKey, Iterable<TaskConfigRow>> getRows =
        new Function<IJobKey, Iterable<TaskConfigRow>>() {
          @Override
          public Iterable<TaskConfigRow> apply(IJobKey job) {
            return configManager.getConfigs(job);
          }
        };

    Map<ITaskConfig, TaskConfigRow> rowsToIds =
        FluentIterable.from(jobs)
            .transformAndConcat(getRows)
            .uniqueIndex(Functions.compose(ITaskConfig.FROM_BUILDER, getConfigSaturator()));

    return Maps.transformValues(rowsToIds, CONFIG_ID);
  }

  @Timed("db_storage_save_tasks")
  @Override
  public void saveTasks(Set<IScheduledTask> tasks) {
    if (tasks.isEmpty()) {
      return;
    }

    // TODO(wfarner): Restrict the TaskStore.Mutable methods to more specific mutations.  It would
    //                simplify this code if we did not have to handle full object tree mutations.

    deleteTasks(Tasks.ids(tasks));

    // Maintain a cache of all task configs that exist for a job key so that identical entities
    LoadingCache<ITaskConfig, Long> configCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<ITaskConfig, Long>() {
          @Override
          public Long load(ITaskConfig config) {
            return configManager.insert(config);
          }
        });

    // Seed the cache with known configs in the jobs being updated.
    configCache.putAll(getTaskConfigRows(
        FluentIterable.from(tasks)
            .transform(Tasks.SCHEDULED_TO_JOB_KEY)
            .toSet()));

    for (IScheduledTask task : tasks) {
      jobKeyMapper.merge(task.getAssignedTask().getTask().getJob().newBuilder());
      long configId = configCache.getUnchecked(task.getAssignedTask().getTask());

      ScheduledTaskWrapper wrappedTask = new ScheduledTaskWrapper(-1, configId, task.newBuilder());
      taskMapper.insertScheduledTask(wrappedTask);
      Preconditions.checkState(
          wrappedTask.getTaskRowId() != -1,
          "Row ID should have been populated during insert.");
      if (!task.getTaskEvents().isEmpty()) {
        taskMapper.insertTaskEvents(wrappedTask.getTaskRowId(), task.getTaskEvents());
      }
      if (!task.getAssignedTask().getAssignedPorts().isEmpty()) {
        taskMapper.insertPorts(
            wrappedTask.getTaskRowId(),
            toAssignedPorts(task.getAssignedTask().getAssignedPorts()));
      }
    }
  }

  private static List<AssignedPort> toAssignedPorts(Map<String, Integer> ports) {
    // Mybatis does not seem to support inserting maps where the keys are not known in advance (it
    // treats them as bags of properties, presumably like a cheap bean object).
    // See https://github.com/mybatis/mybatis-3/pull/208, and seemingly-relevant code in
    // https://github.com/mybatis/mybatis-3/blob/4cfc129938fd6b5cb20c4b741392e8b3fa41b529/src
    // main/java/org/apache/ibatis/scripting/xmltags/ForEachSqlNode.java#L73-L77.
    ImmutableList.Builder<AssignedPort> list = ImmutableList.builder();
    for (Map.Entry<String, Integer> entry : ports.entrySet()) {
      list.add(new AssignedPort(entry.getKey(), entry.getValue()));
    }
    return list.build();
  }

  @Timed("db_storage_delete_all_tasks")
  @Override
  public void deleteAllTasks() {
    // TODO(wfarner): Need to re-evaluate all task configs after deleting tasks.
    taskMapper.truncate();
  }

  @Timed("db_storage_delete_tasks")
  @Override
  public void deleteTasks(Set<String> taskIds) {
    if (!taskIds.isEmpty()) {
      // First fetch task configs referenced by these task IDs.
      List<Long> configIds = configManager.getTaskConfigIds(taskIds);

      taskMapper.deleteTasks(taskIds);

      if (!configIds.isEmpty()) {
        configManager.maybeExpungeConfigs(ImmutableSet.copyOf(configIds));
      }
    }
  }

  @Timed("db_storage_mutate_tasks")
  @Override
  public ImmutableSet<IScheduledTask> mutateTasks(
      Builder query,
      Function<IScheduledTask, IScheduledTask> mutator) {

    requireNonNull(query);
    requireNonNull(mutator);

    ImmutableSet.Builder<IScheduledTask> mutated = ImmutableSet.builder();
    for (IScheduledTask original : fetchTasks(query)) {
      IScheduledTask maybeMutated = mutator.apply(original);
      if (!original.equals(maybeMutated)) {
        Preconditions.checkState(
            Tasks.id(original).equals(Tasks.id(maybeMutated)),
            "A task's ID may not be mutated.");
        saveTasks(ImmutableSet.of(maybeMutated));
        mutated.add(maybeMutated);
      }
    }

    return mutated.build();
  }

  @Timed("db_storage_unsafe_modify_in_place")
  @Override
  public boolean unsafeModifyInPlace(String taskId, ITaskConfig taskConfiguration) {
    checkNotNull(taskId);
    checkNotNull(taskConfiguration);
    Optional<IScheduledTask> task =
        Optional.fromNullable(Iterables.getOnlyElement(fetchTasks(Query.taskScoped(taskId)), null));
    if (task.isPresent()) {
      deleteTasks(ImmutableSet.of(taskId));
      ScheduledTask builder = task.get().newBuilder();
      builder.getAssignedTask().setTask(taskConfiguration.newBuilder());
      saveTasks(ImmutableSet.of(IScheduledTask.build(builder)));
      return true;
    }
    return false;
  }

  private Function<TaskConfigRow, TaskConfig> getConfigSaturator() {
    // It appears that there is no way in mybatis to populate a field of type Map.  To work around
    // this, we need to manually perform the query and associate the elements.
    final LoadingCache<Long, Map<String, String>> taskLinkCache = CacheBuilder.newBuilder()
        .build(new CacheLoader<Long, Map<String, String>>() {
          @Override
          public Map<String, String> load(Long configId) {
            return configManager.getTaskLinks(configId);
          }
        });
    Function<TaskConfigRow, TaskConfig> linkPopulator = new Function<TaskConfigRow, TaskConfig>() {
      @Override
      public TaskConfig apply(TaskConfigRow row) {
        return row.getConfig().setTaskLinks(taskLinkCache.getUnchecked(row.getId()));
      }
    };

    return Functions.compose(REPLACE_UNION_TYPES, linkPopulator);
  }

  private FluentIterable<IScheduledTask> matches(Query.Builder query) {
    Iterable<ScheduledTaskWrapper> results;
    Predicate<IScheduledTask> filter;
    if (query.get().getTaskIdsSize() == 1) {
      // Optimize queries that are scoped to a single task, as the dynamic SQL used for arbitrary
      // queries comes with a performance penalty.
      results = Optional.fromNullable(
          taskMapper.selectById(Iterables.getOnlyElement(query.get().getTaskIds())))
          .asSet();
      filter = Util.queryFilter(query);
    } else {
      results = taskMapper.select(query.get());
      // Additional filtering is not necessary in this case, since the query does it for us.
      filter = Predicates.alwaysTrue();
    }

    final Function<TaskConfigRow, TaskConfig> configSaturator = getConfigSaturator();
    return FluentIterable.from(results)
        .transform(populateAssignedPorts)
        .transform(new Function<ScheduledTaskWrapper, ScheduledTaskWrapper>() {
          @Override
          public ScheduledTaskWrapper apply(ScheduledTaskWrapper task) {
            configSaturator.apply(
                new TaskConfigRow(
                    task.getTaskConfigRowId(),
                    task.getTask().getAssignedTask().getTask()));
            return task;
          }
        })
        .transform(UNWRAP)
        .transform(IScheduledTask.FROM_BUILDER)
        .filter(filter);
  }

  private final Function<ScheduledTaskWrapper, ScheduledTaskWrapper> populateAssignedPorts =
      new Function<ScheduledTaskWrapper, ScheduledTaskWrapper>() {
        @Override
        public ScheduledTaskWrapper apply(ScheduledTaskWrapper task) {
          ImmutableMap.Builder<String, Integer> ports = ImmutableMap.builder();
          for (AssignedPort port : taskMapper.selectPorts(task.getTaskRowId())) {
            ports.put(port.getName(), port.getPort());
          }
          task.getTask().getAssignedTask().setAssignedPorts(ports.build());
          return task;
        }
      };

  private static final Function<ScheduledTaskWrapper, ScheduledTask> UNWRAP =
      new Function<ScheduledTaskWrapper, ScheduledTask>() {
        @Override
        public ScheduledTask apply(ScheduledTaskWrapper task) {
          return task.getTask();
        }
      };

  /**
   * Replaces the shimmed {@link org.apache.thrift.TUnion} instances with the base thrift types.
   * This is necessary because TUnion, as of thrift 0.9.1, restricts subclassing.  The copy
   * constructor checks for equality on {@link Object#getClass()} rather than the subclass-friendly
   * {@link Class#isInstance(Object).
   */
  private static final Function<TaskConfig, TaskConfig> REPLACE_UNION_TYPES =
      new Function<TaskConfig, TaskConfig>() {
        @Override
        public TaskConfig apply(TaskConfig config) {
          ImmutableSet.Builder<Constraint> constraints = ImmutableSet.builder();
          for (Constraint constraint : config.getConstraints()) {
            Constraint replacement = new Constraint()
                .setName(constraint.getName());
            replacement.setConstraint(
                new TaskConstraint(
                    constraint.getConstraint().getSetField(),
                    constraint.getConstraint().getFieldValue()));
            constraints.add(replacement);
          }
          config.setConstraints(constraints.build());

          config.setContainer(
              new Container(
                  config.getContainer().getSetField(),
                  config.getContainer().getFieldValue()));

          return config;
        }
      };
}
