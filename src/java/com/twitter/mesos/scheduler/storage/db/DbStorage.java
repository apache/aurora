package com.twitter.mesos.scheduler.storage.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionTransporter;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.stats.StatImpl;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ConfiguratonKey;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.StorageMigrationResult;
import com.twitter.mesos.gen.StorageMigrationResults;
import com.twitter.mesos.gen.StorageMigrationStatus;
import com.twitter.mesos.gen.StorageSystemId;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.MigrationUtils;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.TaskStore;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.springframework.core.io.ClassRelativeResourceLoader;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.annotation.Nullable;

import static com.google.common.collect.Iterables.transform;

/**
 * A task store that saves data to a database with a JDBC driver.
 *
 * @author John Sirois
 */
public class DbStorage implements Storage, SchedulerStore, JobStore, TaskStore {

  private static final Logger LOG = Logger.getLogger(DbStorage.class.getName());

  /**
   * The {@link com.twitter.mesos.gen.StorageSystemId#getType() type} identifier for
   * {@code DbStorage}.
   */
  public static final String STORAGE_SYSTEM_TYPE = "EMBEDDED_H2_DB";

  /**
   * The version of this {@code DbStorage}.  Should be bumped when any combination of database
   * implementation change and/or schema change requires a data migration.
   */
  static final int STORAGE_SYSTEM_VERSION = 0;

  private static final StorageSystemId ID =
      new StorageSystemId(STORAGE_SYSTEM_TYPE, STORAGE_SYSTEM_VERSION);

  @VisibleForTesting final JdbcTemplate jdbcTemplate;
  private final TransactionTemplate transactionTemplate;

  /**
   * @param jdbcTemplate The {@code JdbcTemplate} object to execute database operation against.
   * @param transactionTemplate The {@code TransactionTemplate} object that provides transaction
   *     scope for database operations.
   */
  @Inject
  public DbStorage(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
    this.jdbcTemplate = Preconditions.checkNotNull(jdbcTemplate);
    this.transactionTemplate = Preconditions.checkNotNull(transactionTemplate);
  }

  @Override
  public StorageSystemId id() {
    return ID;
  }

  @Override
  public void start(final Work.NoResult.Quiet initilizationLogic) {
    doInTransaction(new Work.NoResult.Quiet() {
      @Override protected void execute(SchedulerStore schedulerStore, JobStore jobStore,
          TaskStore taskStore) throws RuntimeException {

        ClassRelativeResourceLoader resourceLoader = new ClassRelativeResourceLoader(getClass());
        URL schemaUrl;
        try {
          schemaUrl = resourceLoader.getResource("db-task-store-schema.sql").getURL();
        } catch (IOException e) {
          throw new IllegalStateException("Could not find schema on classpath", e);
        }
        jdbcTemplate.execute(String.format("RUNSCRIPT FROM '%s'", schemaUrl));

        initilizationLogic.apply(schedulerStore, jobStore, taskStore);
      }
    });
  }

  @Timed("db_storage_mark_migration")
  @Override
  public void markMigration(final StorageMigrationResult result) {
    Preconditions.checkNotNull(result);

    updateSchedulerState(ConfiguratonKey.MIGRATION_RESULTS, new StorageMigrationResults(),
        new Closure<StorageMigrationResults>() {
          @Override public void execute(StorageMigrationResults fetched) {
            fetched.putToResult(result.getPath(), result);
          }
        });
  }

  @Timed("db_storage_has_migrated")
  @Override
  public boolean hasMigrated(Storage from) {
    Preconditions.checkNotNull(from);

    StorageMigrationResults fetched =
        fetchSchedulerState(ConfiguratonKey.MIGRATION_RESULTS, new StorageMigrationResults());

    if (!fetched.isSetResult()) {
      return false;
    }

    StorageMigrationResult result = fetched.result.get(MigrationUtils.migrationPath(from, this));
    return (result != null) && (result.status == StorageMigrationStatus.SUCCESS);
  }

  @Override
  public <T, E extends Exception> T doInTransaction(final Work<T, E> work) throws E {
    Preconditions.checkNotNull(work);

    final DbStorage self = this;
    return ExceptionTransporter.guard(new Function<ExceptionTransporter<E>, T>() {
      @Override public T apply(final ExceptionTransporter<E> transporter) {
        return transactionTemplate.execute(new TransactionCallback<T>() {
          @Override public T doInTransaction(TransactionStatus transactionStatus) {
            try {
              return work.apply(self, self, self);
            } catch (Exception e) {
              // We know work throws E by its signature
              @SuppressWarnings("unchecked")
              E exception = (E) e;
              throw transporter.transport(exception);
            }
          }
        });
      }
    });
  }

  @Override
  public void stop() {
    // noop
  }

  @Timed("db_storage_save_framework_id")
  @Override
  public void saveFrameworkId(final String frameworkId) {
    MorePreconditions.checkNotBlank(frameworkId);

    updateSchedulerState(com.twitter.mesos.gen.ConfiguratonKey.FRAMEWORK_ID,
        new ExceptionalClosure<TProtocol, TException>() {
          @Override public void execute(TProtocol stream) throws TException {
            stream.writeString(frameworkId);
          }
        });
  }

  @Timed("db_storage_fetch_framework_id")
  @Override
  @Nullable
  public String fetchFrameworkId() {
    return fetchSchedulerState(com.twitter.mesos.gen.ConfiguratonKey.FRAMEWORK_ID,
        new ExceptionalFunction<TProtocol, String, TException>() {
          @Override public String apply(TProtocol data) throws TException {
            return data.readString();
          }
        }, null);
  }

  private <T extends TBase<?, ?>> void updateSchedulerState(final ConfiguratonKey key,
      final T blank, final Closure<T> mutator) {

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        final T state = fetchSchedulerState(key, blank);
        mutator.execute(state);
        updateSchedulerState(key, new ExceptionalClosure<TProtocol, TException>() {
          @Override public void execute(TProtocol data) throws TException {
            state.write(data);
          }
        });
      }
    });
  }

  private void updateSchedulerState(final ConfiguratonKey key,
      final ExceptionalClosure<TProtocol, TException> serializationOp) {

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        ByteArrayOutputStream data = new ByteArrayOutputStream();
        TIOStreamTransport transport = new TIOStreamTransport(data);
        try {
          serializationOp.execute(ThriftBinaryCodec.PROTOCOL_FACTORY.getProtocol(transport));
        } catch (TException e) {
          throw new IllegalStateException("Failed to serialize thrift data", e);
        }
        jdbcTemplate.update("MERGE INTO scheduler_state (key, value) KEY(key) VALUES(?, ?)",
            key.getValue(), data.toByteArray());
      }
    });
  }

  private <T extends TBase<?, ?>> T fetchSchedulerState(ConfiguratonKey key, final T blank) {
    return fetchSchedulerState(key, new ExceptionalFunction<TProtocol, T, TException>() {
      @Override public T apply(TProtocol data) throws TException {
        blank.read(data);
        return blank;
      }
    }, blank);
  }

  private <T> T fetchSchedulerState(final ConfiguratonKey key,
      final ExceptionalFunction<TProtocol, T, TException> decoder, @Nullable final T defaultValue) {

    return transactionTemplate.execute(new TransactionCallback<T>() {
      @Override public T doInTransaction(TransactionStatus transactionStatus) {
        List<T> results = jdbcTemplate.query("SELECT value FROM scheduler_state WHERE key = ?",
            new RowMapper<T>() {
              @Override public T mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
                byte[] data = resultSet.getBytes(1);
                TIOStreamTransport transport =
                    new TIOStreamTransport(new ByteArrayInputStream(data));
                try {
                  return decoder.apply(ThriftBinaryCodec.PROTOCOL_FACTORY.getProtocol(transport));
                } catch (TException e) {
                  throw new IllegalStateException("Failed to deserialize thrift data", e);
                }
              }
            }, key.getValue());
        return Iterables.getOnlyElement(results, defaultValue);
      }
    });
  }

  @Timed("db_storage_fetch_jobs")
  @Override
  public Iterable<JobConfiguration> fetchJobs(final String managerId) {
    MorePreconditions.checkNotBlank(managerId);

    List<JobConfiguration> fetched =
        transactionTemplate.execute(new TransactionCallback<List<JobConfiguration>>() {
          @Override
          public List<JobConfiguration> doInTransaction(TransactionStatus transactionStatus) {
            return queryJobs(managerId);
          }
        });
    vars.jobsFetched.addAndGet(fetched.size());
    return fetched;
  }

  @Timed("db_storage_fetch_job")
  @Override
  public JobConfiguration fetchJob(final String managerId, final String jobKey) {
    MorePreconditions.checkNotBlank(managerId);
    MorePreconditions.checkNotBlank(jobKey);

    return transactionTemplate.execute(new TransactionCallback<JobConfiguration>() {
      @Override public JobConfiguration doInTransaction(TransactionStatus transactionStatus) {
        return queryJob(managerId, jobKey);
      }
    });
  }

  private static final RowMapper<JobConfiguration> JOB_CONFIGURATION_ROW_MAPPER =
      new RowMapper<JobConfiguration>() {
        @Override public JobConfiguration mapRow(ResultSet resultSet, int rowIndex)
            throws SQLException {

          try {
            return ThriftBinaryCodec.decode(JobConfiguration.class, resultSet.getBytes(1));
          } catch (CodingException e) {
            throw new SQLException("Problem decoding JobConfiguration", e);
          }
        }
      };

  private List<JobConfiguration> queryJobs(String managerId) {
    return jdbcTemplate.query("SELECT job_configuration FROM job_state WHERE manager_id = ?",
        JOB_CONFIGURATION_ROW_MAPPER, managerId);
  }

  @Nullable
  private JobConfiguration queryJob(String managerId, String jobKey) {
    List<JobConfiguration> results =
        jdbcTemplate.query(
            "SELECT job_configuration FROM job_state WHERE manager_id = ? AND job_key = ?",
            JOB_CONFIGURATION_ROW_MAPPER, managerId, jobKey);
    return Iterables.getOnlyElement(results, null);
  }

  @Timed("db_storage_save_accepted_job")
  @Override
  public void saveAcceptedJob(final String managerId, final JobConfiguration jobConfig) {
    MorePreconditions.checkNotBlank(managerId);
    Preconditions.checkNotNull(jobConfig);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        // TODO(William Farner): consider adding a checksum column to verify job_configuration bytes
        jdbcTemplate.update(
            "INSERT INTO job_state (job_key, manager_id, job_configuration) VALUES (?, ?, ?)",
            Tasks.jobKey(jobConfig), managerId, getBytes(jobConfig));
      }
    });
  }

  @Timed("db_storage_delete_job")
  @Override
  public void deleteJob(final String jobKey) {
    MorePreconditions.checkNotBlank(jobKey);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        jdbcTemplate.update("DELETE FROM job_state WHERE job_key = ?", jobKey);
      }
    });
  }

  @Timed("db_storage_add_tasks")
  @Override
  public void add(final Set<ScheduledTask> newTasks) {
    Preconditions.checkNotNull(newTasks);
    Preconditions.checkState(
        Sets.newHashSet(transform(newTasks, Tasks.SCHEDULED_TO_ID)).size() == newTasks.size(),
        "Proposed new tasks would create task ID collision.");

    // Do a first pass to make sure all of the values are good.
    for (ScheduledTask task : newTasks) {
      Preconditions.checkNotNull(task.getAssignedTask(), "Assigned task may not be null.");
      Preconditions.checkNotNull(task.getAssignedTask().getTask(), "Task info may not be null.");
    }

    if(!newTasks.isEmpty()) {
      transactionTemplate.execute(new TransactionCallbackWithoutResult() {
        @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
          insert(newTasks);
        }
      });
      vars.tasksAdded.addAndGet(newTasks.size());
    }
  }

  private void insert(final Set<ScheduledTask> newTasks) {
    final Iterator<ScheduledTask> tasks = newTasks.iterator();
    try {
      jdbcTemplate.batchUpdate("INSERT INTO task_state (task_id, owner, job_name, job_key,"
                               + " slave_host, shard_id, status, scheduled_task) VALUES"
                               + " (?, ?, ?, ?, ?, ?, ?, ?)",
          new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement preparedStatement, int batchItemIndex)
                throws SQLException {

              ScheduledTask scheduledTask = tasks.next();
              setTaskId(preparedStatement, 1, scheduledTask);
              prepareRow(preparedStatement, 2, scheduledTask);
            }

            @Override public int getBatchSize() {
              return newTasks.size();
            }
          });
    } catch (DuplicateKeyException e) {
      throw new IllegalStateException(e);
    }
  }

  @Timed("db_storage_remove_tasks")
  @Override
  public void remove(final Query query) {
    Preconditions.checkNotNull(query);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        if (query.hasPostFilter()) {
          remove(fetchIds(query));
        } else {
          remove(createWhereClause(query));
        }
      }
    });
  }

  @Timed("db_storage_remove_tasks_by_id")
  @Override
  public void remove(final Set<String> taskIds) {
    if (!taskIds.isEmpty()) {
      LOG.info("Removing tasks: " + taskIds);
      transactionTemplate.execute(new TransactionCallbackWithoutResult() {
        @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
          remove(restrictTaskIds(new WhereClauseBuilder(), taskIds));
        }
      });
      vars.tasksRemoved.addAndGet(taskIds.size());
    }
  }

  private void remove(WhereClauseBuilder whereClauseBuilder) {
    StringBuilder sql = new StringBuilder("DELETE FROM task_state");
    String sqlString = whereClauseBuilder.appendWhereClause(sql).toString();

    LOG.log(Level.INFO, "Removing tasks matching: {0} {1}",
        new Object[] { sqlString, whereClauseBuilder.parameters });

    int removeCount = jdbcTemplate.update(sqlString,
        whereClauseBuilder.parameters(), whereClauseBuilder.parameterTypes());
    vars.tasksRemoved.addAndGet(removeCount);
  }

  @Timed("db_storage_mutate_tasks")
  @Override
  public ImmutableSet<ScheduledTask> mutate(final Query query,
      final Closure<ScheduledTask> mutator) {

    Preconditions.checkNotNull(query);
    Preconditions.checkNotNull(mutator);

    return transactionTemplate.execute(new TransactionCallback<ImmutableSet<ScheduledTask>>() {
      @Override public ImmutableSet<ScheduledTask> doInTransaction(TransactionStatus status) {
        return update(query, mutator);
      }
    });
  }

  private ImmutableSet<ScheduledTask> update(Query query, Closure<ScheduledTask> mutator) {
    final ImmutableSortedSet<ScheduledTask> taskStates = fetch(query);
    for (ScheduledTask taskState : taskStates) {
      mutator.execute(taskState);
    }

    final Iterator<ScheduledTask> tasks = taskStates.iterator();
    jdbcTemplate.batchUpdate("UPDATE task_state SET owner = ?, job_name = ?,"
                             + " job_key = ?, slave_host = ?, shard_id = ?, status = ?,"
                             + " scheduled_task = ? WHERE task_id = ?",
        new BatchPreparedStatementSetter() {
          @Override public void setValues(PreparedStatement preparedStatement, int batchItemIndex)
              throws SQLException {

            ScheduledTask scheduledTask = tasks.next();

            int col = prepareRow(preparedStatement, 1, scheduledTask);
            setTaskId(preparedStatement, col, scheduledTask);
          }

          @Override public int getBatchSize() {
            return taskStates.size();
          }
        });

    // TODO(John Sirois): detect real mutations, some or all of these may have been noops
    vars.tasksMutated.addAndGet(taskStates.size());

    return taskStates;
  }

  @Timed("db_storage_fetch_tasks")
  @Override
  public ImmutableSortedSet<ScheduledTask> fetch(final Query query) {
    Preconditions.checkNotNull(query);

    ImmutableSortedSet<ScheduledTask> fetched =
        transactionTemplate.execute(new TransactionCallback<ImmutableSortedSet<ScheduledTask>>() {
          @Override
          public ImmutableSortedSet<ScheduledTask> doInTransaction(TransactionStatus status) {
            return ImmutableSortedSet.copyOf(Query.SORT_BY_TASK_ID, query(query));
          }
        });
    vars.tasksFetched.addAndGet(fetched.size());
    return fetched;
  }

  @Timed("db_storage_fetch_task_ids")
  @Override
  public Set<String> fetchIds(final Query query) {
    Preconditions.checkNotNull(query);

    Set<String> fetched = transactionTemplate.execute(new TransactionCallback<Set<String>>() {
      @Override public Set<String> doInTransaction(TransactionStatus status) {
        return ImmutableSet.copyOf(Iterables.transform(query(query), Tasks.SCHEDULED_TO_ID));
      }
    });
    vars.taskIdsFetched.addAndGet(fetched.size());
    return fetched;
  }

  private static boolean isEmpty(@Nullable Collection<?> items) {
    return (items == null) || items.isEmpty();
  }

  private Iterable<ScheduledTask> query(Query query) {
    StringBuilder sqlBuilder = new StringBuilder("SELECT scheduled_task FROM task_state");

    WhereClauseBuilder whereClauseBuilder = createWhereClause(query);
    String rawQuery = whereClauseBuilder.appendWhereClause(sqlBuilder).toString();

    long queryStart = System.nanoTime();
    List<ScheduledTask> results = jdbcTemplate.query(
        rawQuery,
        whereClauseBuilder.parameters(),
        whereClauseBuilder.parameterTypes(),
        new RowMapper<ScheduledTask>() {
          @Override public ScheduledTask mapRow(ResultSet resultSet, int rowIndex)
              throws SQLException {

            ScheduledTask scheduledTask;
            try {
              scheduledTask = ThriftBinaryCodec.decode(ScheduledTask.class, resultSet.getBytes(1));
            } catch (CodingException e) {
              throw new SQLException("Problem decoding ScheduledTask", e);
            }

            return scheduledTask;
          }
        });
    long queryDuration = System.nanoTime() - queryStart;

    long postFilterStart = System.nanoTime();
    Iterable<ScheduledTask> postFiltered = query.hasPostFilter()
        ? Iterables.filter(results, query.postFilter()) : results;
    long postFilterDuration = System.nanoTime() - postFilterStart;
    LOG.info("Query '" + rawQuery + "' completed in " + queryDuration + "ns, and took "
        + postFilterDuration + "ns to post-filter");

    return postFiltered;
  }

  /**
   * A sql where clause builder that supports separating collection of restrictions from rendering
   * of a sql statement.  Restrictions are taken as pure conjunctive.
   */
  static class WhereClauseBuilder {
    private final List<String> fragments = Lists.newArrayList();
    private final List<Object> parameters = Lists.newArrayList();
    private final List<Integer> parameterTypes = Lists.newArrayList();

    WhereClauseBuilder equals(String column, int sqlType, Object value) {
      fragments.add(column + " = ?");
      parameters.add(value);
      parameterTypes.add(sqlType);
      return this;
    }

    <T> WhereClauseBuilder in(String column, int sqlType, Collection<T> values) {
      return in(column, sqlType, values, Functions.<T>identity());
    }

    <T> WhereClauseBuilder in(String column, int sqlType, Collection<T> values,
        Function<T, ?> transform) {

      fragments.add(String.format(column + " in (%s)", Joiner.on(", ")
          .join(Iterables.limit(Iterables.cycle("?"), values.size()))));
      Iterables.addAll(parameters, Iterables.transform(values, transform));
      Iterables.addAll(parameterTypes, Iterables.limit(Iterables.cycle(sqlType), values.size()));
      return this;
    }

    StringBuilder appendWhereClause(StringBuilder sql) {
      if (!fragments.isEmpty()) {
        sql.append(" where ");
        Joiner.on(" and ").appendTo(sql, fragments);
      }
      return sql;
    }

    Object[] parameters() {
      return parameters.toArray(new Object[parameters.size()]);
    }

    int[] parameterTypes() {
      return Ints.toArray(parameterTypes);
    }
  }

  private static WhereClauseBuilder createWhereClause(Query query) {
    // TODO(John Sirois): investigate using:
    // org.springframework.jdbc.core.namedparam.MapSqlParameterSource
    WhereClauseBuilder whereClauseBuilder = new WhereClauseBuilder();

    TaskQuery taskQuery = query.base();
    if (!StringUtils.isEmpty(taskQuery.getOwner())) {
      whereClauseBuilder.equals("owner", Types.VARCHAR, taskQuery.getOwner());
    }
    if (!StringUtils.isEmpty(taskQuery.getJobName())) {
      whereClauseBuilder.equals("job_name", Types.VARCHAR, taskQuery.getJobName());
    }
    if (!StringUtils.isEmpty(taskQuery.getJobKey())) {
      whereClauseBuilder.equals("job_key", Types.VARCHAR, taskQuery.getJobKey());
    }

    // MapStorage currently has the semantics that null taskIds skips the restiction, but empty
    // taskIds applies the always unsatisfiable restriction - we emulate this here by generating the
    // query clause 'where ... task_id in () ...' but the semantics seem confusing - address this.
    if (taskQuery.getTaskIds() != null) {
      restrictTaskIds(whereClauseBuilder, taskQuery.getTaskIds());
    }

    if (!isEmpty(taskQuery.getStatuses())) {
      whereClauseBuilder.in("status", Types.INTEGER, taskQuery.getStatuses(),
          new Function<ScheduleStatus, Integer>() {
            @Override public Integer apply(ScheduleStatus status) {
              return status.getValue();
            }
          });
    }
    if (!StringUtils.isEmpty(taskQuery.getSlaveHost())) {
      whereClauseBuilder.equals("slave_host", Types.VARCHAR, taskQuery.getSlaveHost());
    }
    if (!isEmpty(taskQuery.getShardIds())) {
      whereClauseBuilder.in("shard_id", Types.INTEGER, taskQuery.getShardIds());
    }

    return whereClauseBuilder;
  }

  private static WhereClauseBuilder restrictTaskIds(WhereClauseBuilder whereClauseBuilder,
      Collection<String> taskIds) {

    return whereClauseBuilder.in("task_id", Types.VARCHAR, taskIds);
  }

  private static int setTaskId(PreparedStatement preparedStatement, int col,
      ScheduledTask scheduledTask) throws SQLException {

    preparedStatement.setString(col++, scheduledTask.assignedTask.taskId);

    return col;
  }

  private static int prepareRow(PreparedStatement preparedStatement, int col,
      ScheduledTask scheduledTask) throws SQLException {

    setString(preparedStatement, col++, scheduledTask.assignedTask.task.owner);
    setString(preparedStatement, col++, scheduledTask.assignedTask.task.jobName);
    setString(preparedStatement, col++, Tasks.jobKey(scheduledTask));
    setString(preparedStatement, col++, scheduledTask.assignedTask.slaveHost);
    preparedStatement.setInt(col++, scheduledTask.assignedTask.task.shardId);
    preparedStatement.setInt(col++, scheduledTask.status.getValue());
    setBytes(preparedStatement, col++, scheduledTask);

    return col;
  }

  private static void setString(PreparedStatement preparedStatement, int col,
      @Nullable String value) throws SQLException {

    if (value == null) {
      preparedStatement.setNull(col, Types.VARCHAR);
    } else {
      preparedStatement.setString(col, value);
    }
  }

  private static void setBytes(PreparedStatement preparedStatement, int col,
      @Nullable TBase struct) throws SQLException {

    if (struct == null) {
      preparedStatement.setNull(col, Types.BINARY);
    } else {
      preparedStatement.setBytes(col, getBytes(struct));
    }
  }

  private static byte[] getBytes(TBase struct) {
    try {
      return ThriftBinaryCodec.encode(struct);
    } catch (CodingException e) {
      throw new IllegalStateException("Unexpected exception serializing a ScheduledTask struct", e);
    }
  }

  private class Vars {
    private final AtomicLong tasksAdded = Stats.exportLong("task_store_tasks_added");
    private final AtomicLong tasksRemoved = Stats.exportLong("task_store_tasks_removed");
    private final AtomicLong tasksMutated = Stats.exportLong("task_store_tasks_mutated");
    private final AtomicLong tasksFetched = Stats.exportLong("task_store_tasks_fetched");
    private final AtomicLong taskIdsFetched = Stats.exportLong("task_store_task_ids_fetched");
    private final AtomicLong jobsFetched = Stats.exportLong("job_store_jobs_fetched");

    Vars() {
      Stats.export(new StatImpl<Integer>("task_store_size") {
        @Override public Integer read() {
          return getTaskStoreSize();
        }
      });
    }
  }

  private final Vars vars = new Vars();

  @VisibleForTesting
  int getTaskStoreSize() {
    return transactionTemplate.execute(new TransactionCallback<Integer>() {
      @Override public Integer doInTransaction(TransactionStatus transactionStatus) {
        return jdbcTemplate.queryForInt("SELECT COUNT(task_id) FROM task_state");
      }
    });
  }
}
