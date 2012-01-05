package com.twitter.mesos.scheduler.storage.db;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;

import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import com.twitter.common.base.Closure;
import com.twitter.common.base.ExceptionTransporter;
import com.twitter.common.base.ExceptionalClosure;
import com.twitter.common.base.ExceptionalFunction;
import com.twitter.common.inject.TimedInterceptor.Timed;
import com.twitter.common.io.FileUtils;
import com.twitter.common.io.FileUtils.Temporary;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Time;
import com.twitter.common.stats.Stats;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.JobConfiguration;
import com.twitter.mesos.gen.Quota;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import com.twitter.mesos.gen.storage.ConfiguratonKey;
import com.twitter.mesos.gen.storage.TaskUpdateConfiguration;
import com.twitter.mesos.scheduler.Query;
import com.twitter.mesos.scheduler.db.DbUtil;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.SnapshotStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static com.twitter.common.base.MorePreconditions.checkNotBlank;

/**
 * A task store that saves data to a database with a JDBC driver.
 *
 * @author John Sirois
 */
public class DbStorage implements
    SnapshotStore<byte[]>,
    Storage,
    SchedulerStore,
    JobStore,
    TaskStore,
    UpdateStore,
    QuotaStore {

  private static final Logger LOG = Logger.getLogger(DbStorage.class.getName());

  private static final long SLOW_QUERY_THRESHOLD_NS =
      Amount.of(100L, Time.MILLISECONDS).as(Time.NANOSECONDS);

  @VisibleForTesting final JdbcTemplate jdbcTemplate;
  private final TransactionTemplate transactionTemplate;
  private final Temporary temporary = FileUtils.SYSTEM_TMP;
  private boolean initialized;

  private final DbStorage self = this;
  private final StoreProvider storeProvider = new StoreProvider() {
    @Override public SchedulerStore getSchedulerStore() {
      return self;
    }
    @Override public JobStore getJobStore() {
      return self;
    }
    @Override public TaskStore getTaskStore() {
      return self;
    }
    @Override public UpdateStore getUpdateStore() {
      return self;
    }
    @Override public QuotaStore getQuotaStore() {
      return self;
    }
  };

  /**
   * @param jdbcTemplate The {@code JdbcTemplate} object to execute database operation against.
   * @param transactionTemplate The {@code TransactionTemplate} object that provides transaction
   *     scope for database operations.
   */
  @Inject
  public DbStorage(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
    this.jdbcTemplate = checkNotNull(jdbcTemplate);
    this.transactionTemplate = checkNotNull(transactionTemplate);
  }

  // TODO(wfarner): Remove this code once schema has been updated in all clusters.
  @VisibleForTesting
  boolean isOldUpdateStoreSchema() {
    return !jdbcTemplate.queryForList(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'UPDATE_STORE'"
            + " AND COLUMN_NAME = 'JOB_KEY'", String.class).isEmpty();
  }

  @VisibleForTesting
  void maybeUpgradeUpdateStoreSchema() {
    if (isOldUpdateStoreSchema()) {
      LOG.warning("Old update_store schema found, DROPPING.");
      jdbcTemplate.execute("DROP TABLE IF EXISTS update_store");
      jdbcTemplate.execute("DROP INDEX IF EXISTS update_store_job_key_shard_id_idx");
    }
  }

  @VisibleForTesting
  void createSchema() {
    executeSql(new ClassPathResource("db-task-store-schema.sql", getClass()), false);
    LOG.info("Initialized schema.");
  }

  public synchronized void ensureInitialized() {
    if (!initialized) {
      maybeUpgradeUpdateStoreSchema();

      createSchema();
      initialized = true;

      Preconditions.checkState(!isOldUpdateStoreSchema(), "Update store is using old schema!");
    }
  }

  public void executeSql(final ClassPathResource sqlResource, final boolean logSql) {
    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        DbUtil.executeSql(jdbcTemplate, sqlResource, logSql);
      }
    });
  }

  @Override
  public void prepare() {
    // Nothing to do.
  }

  @Override
  public void start(final Work.NoResult.Quiet initilizationLogic) {
    checkNotNull(initilizationLogic);

    doInTransaction(new Work.NoResult.Quiet() {
      @Override
      protected void execute(StoreProvider storeProvider) {
        ensureInitialized();

        initilizationLogic.apply(storeProvider);
        LOG.info("Applied initialization logic.");
      }
    });
  }

  @Override
  public <T, E extends Exception> T doInTransaction(final Work<T, E> work)
      throws StorageException, E {

    checkNotNull(work);

    try {
      return ExceptionTransporter.guard(new Function<ExceptionTransporter<E>, T>() {
        @Override public T apply(final ExceptionTransporter<E> transporter) {
          return transactionTemplate.execute(new TransactionCallback<T>() {
            @Override public T doInTransaction(TransactionStatus transactionStatus) {
              try {
                return work.apply(storeProvider);
              } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "work failed in transaction", e);
                throw e; // no need to transport these
              } catch (Exception e) {
                // TODO(wfarner): We may never actually enter this since StorageException is the
                // most common, and it now extends RuntimeException.
                // We know work throws E by its signature
                @SuppressWarnings("unchecked") E exception = (E) e;
                LOG.log(Level.WARNING, "work failed in transaction", e);
                throw transporter.transport(exception);
              }
            }
          });
        }
      });
    } catch (DataAccessException e) {
      throw new StorageException("Problem reading or writing to stable storage.", e);
    } catch (TransactionException e) {
      throw new StorageException("Problem executing transaction.", e);
    }
  }

  @Override
  public void stop() {
    // noop
  }

  @Timed("db_storage_save_framework_id")
  @Override
  public void saveFrameworkId(final String frameworkId) {
    checkNotBlank(frameworkId);

    updateSchedulerState(ConfiguratonKey.FRAMEWORK_ID,
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
    return fetchSchedulerState(ConfiguratonKey.FRAMEWORK_ID,
        new ExceptionalFunction<TProtocol, String, TException>() {
          @Override public String apply(TProtocol stream) throws TException {
            return stream.readString();
          }
        }, null);
  }

  @Timed("db_storage_create_snapshot")
  @Override
  public byte[] createSnapshot() {
    try {
      return temporary.doWithFile(new ExceptionalFunction<File, byte[], IOException>() {
        @Override public byte[] apply(final File file) throws IOException {
          transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
              jdbcTemplate.execute(
                  String.format("SCRIPT TO '%s' COMPRESSION GZIP CHARSET 'UTF-8'",
                      file.getAbsolutePath()));
            }
          });
          return Files.toByteArray(file);
        }
      });
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Timed("db_storage_apply_snapshot")
  @Override
  public void applySnapshot(final byte[] snapshot) {
    try {
      temporary.doWithFile(new ExceptionalClosure<File, IOException>() {
        @Override public void execute(final File file) throws IOException {
          Files.write(snapshot, file);
          transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
              jdbcTemplate.execute(
                  String.format(
                      "DROP ALL OBJECTS; RUNSCRIPT FROM '%s' COMPRESSION GZIP CHARSET 'UTF-8'",
                      file.getAbsolutePath()));
            }
          });
        }
      });
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
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
    checkNotBlank(managerId);

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
    checkNotBlank(managerId);
    checkNotBlank(jobKey);

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
    checkNotBlank(managerId);
    checkNotNull(jobConfig);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        // TODO(William Farner): consider adding a checksum column to verify job_configuration bytes
        jdbcTemplate.update("MERGE INTO job_state (job_key, manager_id, job_configuration)"
                            + " KEY(job_key) VALUES(?, ?, ?)",
            Tasks.jobKey(jobConfig), managerId, getBytes(jobConfig));
      }
    });
  }

  @Timed("db_storage_delete_job")
  @Override
  public void removeJob(final String jobKey) {
    checkNotBlank(jobKey);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        jdbcTemplate.update("DELETE FROM job_state WHERE job_key = ?", jobKey);
      }
    });
  }

  @Timed("db_storage_add_tasks")
  @Override
  public void saveTasks(final Set<ScheduledTask> tasks) {
    checkNotNull(tasks);
    Preconditions.checkState(
        Sets.newHashSet(transform(tasks, Tasks.SCHEDULED_TO_ID)).size() == tasks.size(),
        "Proposed new tasks would create task ID collision.");

    // Do a first pass to make sure all of the values are good.
    for (ScheduledTask task : tasks) {
      checkNotNull(task.getAssignedTask(), "Assigned task may not be null.");
      checkNotNull(task.getAssignedTask().getTask(), "Task info may not be null.");
    }

    if (!tasks.isEmpty()) {
      transactionTemplate.execute(new TransactionCallbackWithoutResult() {
        @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
          insert(tasks);
        }
      });
      vars.tasksAdded.addAndGet(tasks.size());
    }
  }

  private void insert(final Set<ScheduledTask> newTasks) {
    final Iterator<ScheduledTask> tasks = newTasks.iterator();
    try {
      jdbcTemplate.batchUpdate("MERGE INTO task_state (task_id, job_role, job_user, job_name,"
                               + " job_key, slave_host, shard_id, status,"
                               + " scheduled_task) KEY(task_id)"
                               + " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)",
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
    } catch (DataIntegrityViolationException e) {
      throw new IllegalStateException(e);
    }
  }

  @Timed("db_storage_remove_tasks")
  @Override
  public void removeTasks(final Query query) {
    checkNotNull(query);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        if (query.hasPostFilter()) {
          removeTasks(fetchTaskIds(query));
        } else {
          remove(createWhereClause(query));
        }
      }
    });
  }

  @Timed("db_storage_remove_tasks_by_id")
  @Override
  public void removeTasks(final Set<String> taskIds) {
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

  @Timed("db_storage_add_job_update")
  @Override
  public void saveShardUpdateConfigs(final String role, final String job, final String updateToken,
      final Set<TaskUpdateConfiguration> updateConfiguration) {
    checkNotNull(role);
    checkNotNull(job);
    checkNotNull(updateToken);
    checkNotNull(updateConfiguration);

    if (!updateConfiguration.isEmpty()) {
      transactionTemplate.execute(new TransactionCallbackWithoutResult() {
        @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
          saveUpdateConfig(role, job, updateToken, updateConfiguration);
        }
      });
    }
  }

  private void saveUpdateConfig(final String role, final String job, final String updateToken,
      final Set<TaskUpdateConfiguration> updateConfiguration) {
    final Iterator<TaskUpdateConfiguration> configIterator = updateConfiguration.iterator();
    try {
      jdbcTemplate.batchUpdate(
          "MERGE INTO update_store (job_role, job_name, update_token, shard_id, config)"
              + " KEY(job_role, job_name, shard_id) VALUES(?, ?, ?, ?, ?)",
          new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement preparedStatement, int batchItemIndex)
                throws SQLException {

              TaskUpdateConfiguration config = configIterator.next();

              int shardId = config.getNewConfig() != null ? config.getNewConfig().getShardId() :
                  config.getOldConfig().getShardId();

              setString(preparedStatement, 1, role);
              setString(preparedStatement, 2, job);
              setString(preparedStatement, 3, updateToken);
              setInt(preparedStatement, 4, shardId);
              setBytes(preparedStatement, 5, config);
            }

            @Override public int getBatchSize() {
              return updateConfiguration.size();
            }
          });
    } catch (DataIntegrityViolationException e) {
        throw new IllegalStateException(e);
    }
  }

  @Override
  @Nullable
  public ShardUpdateConfiguration fetchShardUpdateConfig(String role, String job, int shardId) {
    checkNotBlank(role);
    checkNotBlank(job);
    checkNotNull(shardId);

    return Iterables.getOnlyElement(
        fetchShardUpdateConfigs(role, job, ImmutableSet.of(shardId)), null);
  }

  @Timed("db_storage_fetch_shard_update_configs_by_shard")
  @Override
  @Nullable
  public Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(final String role, final String job,
      final Set<Integer> shardIds) {
    checkNotBlank(role);
    checkNotBlank(job);

    return transactionTemplate.execute(new TransactionCallback<Set<ShardUpdateConfiguration>>() {
      @Override public Set<ShardUpdateConfiguration> doInTransaction(
          TransactionStatus transactionStatus) {
        return queryShardUpdateConfigs(role, job, shardIds);
      }
    });
  }

  @Nullable
  private Set<ShardUpdateConfiguration> queryShardUpdateConfigs(String role, String job,
      Set<Integer> shardIds) {
    WhereClauseBuilder whereClauseBuilder = new WhereClauseBuilder()
        .equals("job_role", Types.VARCHAR, role)
        .equals("job_name", Types.VARCHAR, job)
        .in("shard_id", Types.INTEGER, shardIds);

    StringBuilder sqlBuilder = new StringBuilder("SELECT update_token, config FROM update_store");
    return ImmutableSet.copyOf(jdbcTemplate.query(
        whereClauseBuilder.appendWhereClause(sqlBuilder).toString(),
        whereClauseBuilder.parameters(),
        whereClauseBuilder.parameterTypes(),
        SHARD_UPDATE_CONFIG_ROW_MAPPER));
  }

  private static final RowMapper<ShardUpdateConfiguration> SHARD_UPDATE_CONFIG_ROW_MAPPER =
      new RowMapper<ShardUpdateConfiguration>() {
        @Override public ShardUpdateConfiguration mapRow(ResultSet resultSet, int rowIndex)
            throws SQLException {
          try {
            return new ShardUpdateConfiguration(resultSet.getString(1),
                ThriftBinaryCodec.decode(TaskUpdateConfiguration.class, resultSet.getBytes(2)));
          } catch (CodingException e) {
            throw new SQLException("Problem decoding TaskUpdateConfiguration", e);
          }
        }
  };

  @Timed("db_storage_fetch_shard_update_configs_by_job")
  @Override
  public Set<ShardUpdateConfiguration> fetchShardUpdateConfigs(final String role,
      final String job) {
    checkNotBlank(role);
    checkNotBlank(job);

    return transactionTemplate.execute(new TransactionCallback<Set<ShardUpdateConfiguration>>() {
      @Override public Set<ShardUpdateConfiguration> doInTransaction(
          TransactionStatus transactionStatus) {
        return queryShardUpdateConfigs(role, job);
      }
    });
  }

  private static final Function<ShardUpdateConfiguration, String> GET_JOB_NAME =
      new Function<ShardUpdateConfiguration, String>() {
        @Override public String apply(ShardUpdateConfiguration input) {
          return input.getOldConfig() != null ? input.getOldConfig().getJobName()
              : input.getNewConfig().getJobName();
        }
      };

  @Timed("dbOstorage_fetch_shard_update_configs_by_role")
  @Override
  public Multimap<String, ShardUpdateConfiguration> fetchShardUpdateConfigs(final String role) {
    checkNotBlank(role);

    Set<ShardUpdateConfiguration> allConfigs = transactionTemplate.execute(
        new TransactionCallback<Set<ShardUpdateConfiguration>>() {
          @Override public Set<ShardUpdateConfiguration> doInTransaction(
              TransactionStatus transactionStatus) {
            return queryShardUpdateConfigs(role);
          }
        });

    return Multimaps.index(allConfigs, GET_JOB_NAME);
  }

  @Nullable
  private Set<ShardUpdateConfiguration> queryShardUpdateConfigs(String role) {
    return ImmutableSet.copyOf(
        jdbcTemplate.query("SELECT update_token, config FROM update_store WHERE job_role = ?",
                           SHARD_UPDATE_CONFIG_ROW_MAPPER, role));
  }

  @Nullable
  private Set<ShardUpdateConfiguration> queryShardUpdateConfigs(String role, String job) {
    return ImmutableSet.copyOf(jdbcTemplate.query(
        "SELECT update_token, config FROM update_store WHERE job_role = ? AND job_name = ?",
        SHARD_UPDATE_CONFIG_ROW_MAPPER, role, job));
  }

  @Timed("db_storage_remove_job_update")
  @Override
  public void removeShardUpdateConfigs(final String role, final String job) {
    checkNotBlank(role);
    checkNotBlank(job);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus transactionStatus) {
        jdbcTemplate.update(
            "DELETE FROM update_store WHERE job_role = ? AND job_name = ?", role, job);
      }
    });
  }

  private static final RowMapper<Quota> QUOTA_ROW_MAPPER = new RowMapper<Quota>() {
    @Override public Quota mapRow(ResultSet resultSet, int rowIndex) throws SQLException {
      try {
        return ThriftBinaryCodec.decode(Quota.class, resultSet.getBytes(1));
      } catch (CodingException e) {
        throw new SQLException("Problem decoding Quota", e);
      }
    }
  };

  @Timed("db_storage_remove_quota")
  @Override
  public void removeQuota(final String role) {
    checkNotBlank(role);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        jdbcTemplate.update("DELETE FROM quota_store WHERE role = ?", role);
      }
    });
  }

  @Timed("db_storage_save_quota")
  @Override
  public void saveQuota(final String role, final Quota quota) {
    checkNotBlank(role);
    checkNotNull(quota);

    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        jdbcTemplate.update(
            "MERGE INTO quota_store (role, quota) KEY(role) VALUES(?, ?)",
            role, getBytes(quota));
      }
    });
  }

  @Timed("db_storage_fetch_quota")
  @Nullable
  @Override
  public Quota fetchQuota(String role) {
    checkNotBlank(role);

    return Iterables.getOnlyElement(jdbcTemplate.query(
        "SELECT quota FROM quota_store WHERE role = ?",
        QUOTA_ROW_MAPPER,
        role), null);
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
  public ImmutableSet<ScheduledTask> mutateTasks(final Query query,
      final Closure<ScheduledTask> mutator) {

    checkNotNull(query);
    checkNotNull(mutator);

    return transactionTemplate.execute(new TransactionCallback<ImmutableSet<ScheduledTask>>() {
      @Override public ImmutableSet<ScheduledTask> doInTransaction(TransactionStatus status) {
        return doMutate(query, mutator);
      }
    });
  }

  private ImmutableSet<ScheduledTask> doMutate(Query query, Closure<ScheduledTask> mutator) {
    ImmutableSet<ScheduledTask> taskStates = fetchTasks(query);

    ImmutableSet.Builder<ScheduledTask> tasksToUpdateBuilder = ImmutableSet.builder();
    for (ScheduledTask taskState : taskStates) {
      ScheduledTask original = taskState.deepCopy();
      mutator.execute(taskState);
      if (!taskState.equals(original)) {
        tasksToUpdateBuilder.add(taskState);
      }
    }
    final ImmutableSet<ScheduledTask> tasksToUpdate = tasksToUpdateBuilder.build();
    if (!tasksToUpdate.isEmpty()) {
      LOG.info(String.format("Updating %d tasks", tasksToUpdate.size()));

      long startNanos = System.nanoTime();
      final Iterator<ScheduledTask> tasks = tasksToUpdate.iterator();
      jdbcTemplate.batchUpdate("UPDATE task_state SET job_role = ?, job_user = ?, job_name = ?,"
                               + " job_key = ?, slave_host = ?, shard_id = ?,"
                               + " status = ?, scheduled_task = ? WHERE task_id = ?",
          new BatchPreparedStatementSetter() {
            @Override public void setValues(PreparedStatement preparedStatement, int batchItemIndex)
                throws SQLException {

              ScheduledTask scheduledTask = tasks.next();

              int col = prepareRow(preparedStatement, 1, scheduledTask);
              setTaskId(preparedStatement, col, scheduledTask);
            }

            @Override public int getBatchSize() {
              return tasksToUpdate.size();
            }
          });

      long durationNanos = System.nanoTime() - startNanos;
      if (durationNanos >= SLOW_QUERY_THRESHOLD_NS) {
        LOG.warning("Slow update of " + tasksToUpdate.size() + " tasks took "
                    + Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS) + " ms");
      }
    }
    vars.tasksMutated.addAndGet(tasksToUpdate.size());
    return tasksToUpdate;
  }

  @Timed("db_storage_fetch_tasks")
  @Override
  public ImmutableSet<ScheduledTask> fetchTasks(final Query query) {
    checkNotNull(query);

    ImmutableSet<ScheduledTask> fetched =
        transactionTemplate.execute(new TransactionCallback<ImmutableSet<ScheduledTask>>() {
          @Override public ImmutableSet<ScheduledTask> doInTransaction(TransactionStatus status) {
            return ImmutableSet.copyOf(query(query));
          }
        });
    vars.tasksFetched.addAndGet(fetched.size());
    return fetched;
  }

  @Timed("db_storage_fetch_task_ids")
  @Override
  public Set<String> fetchTaskIds(final Query query) {
    checkNotNull(query);

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

    long startNanos = System.nanoTime();
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

    Iterable<ScheduledTask> postFiltered = query.hasPostFilter()
        ? Iterables.filter(results, query.postFilter()) : results;

    long durationNanos = System.nanoTime() - startNanos;
    if (durationNanos >= SLOW_QUERY_THRESHOLD_NS) {
      LOG.warning("Slow query '" + rawQuery + "' completed in "
          + Amount.of(durationNanos, Time.NANOSECONDS).as(Time.MILLISECONDS) + "ms");
    }

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

    if (taskQuery.getOwner() != null) {
      if (!StringUtils.isBlank(taskQuery.getOwner().getRole())) {
        whereClauseBuilder.equals("job_role", Types.VARCHAR, taskQuery.getOwner().getRole());
      }
      if (!StringUtils.isBlank(taskQuery.getOwner().getUser())) {
        whereClauseBuilder.equals("job_user", Types.VARCHAR, taskQuery.getOwner().getUser());
      }
    }
    if (!StringUtils.isEmpty(taskQuery.getJobName())) {
      whereClauseBuilder.equals("job_name", Types.VARCHAR, taskQuery.getJobName());
    }
    if (!StringUtils.isEmpty(taskQuery.getJobKey())) {
      whereClauseBuilder.equals("job_key", Types.VARCHAR, taskQuery.getJobKey());
    }

    // MapStorage currently has the semantics that null taskIds skips the restriction, but empty
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

    setString(preparedStatement, col++, scheduledTask.assignedTask.task.owner.role);
    setString(preparedStatement, col++, scheduledTask.assignedTask.task.owner.user);
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

  private static void setInt(PreparedStatement preparedStatement, int col, int value)
    throws SQLException {
    preparedStatement.setInt(col, value);
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
  }

  private final Vars vars = new Vars();

  @VisibleForTesting
  int getTaskStoreSize() {
    // Stats sampling comes up early - make sure we have a schema to query against.
    ensureInitialized();

    return transactionTemplate.execute(new TransactionCallback<Integer>() {
      @Override public Integer doInTransaction(TransactionStatus transactionStatus) {
        return jdbcTemplate.queryForInt("SELECT COUNT(task_id) FROM task_state");
      }
    });
  }
}
