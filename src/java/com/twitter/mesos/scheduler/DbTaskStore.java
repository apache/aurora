package com.twitter.mesos.scheduler;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.inject.Inject;
import com.twitter.common.base.Closure;
import com.twitter.mesos.Tasks;
import com.twitter.mesos.codec.ThriftBinaryCodec;
import com.twitter.mesos.codec.ThriftBinaryCodec.CodingException;
import com.twitter.mesos.gen.ScheduleStatus;
import com.twitter.mesos.gen.ScheduledTask;
import com.twitter.mesos.gen.TaskQuery;
import org.apache.commons.lang.StringUtils;
import org.apache.thrift.TBase;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;

import javax.annotation.Nullable;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A task store that saves data to a database with a JDBC driver.
 *
 * @author jsirois
 */
public class DbTaskStore implements TaskStore {
  private final JdbcTemplate jdbcTemplate;
  private final TransactionTemplate transactionTemplate;

  @Inject
  public DbTaskStore(JdbcTemplate jdbcTemplate, TransactionTemplate transactionTemplate) {
    this.jdbcTemplate = Preconditions.checkNotNull(jdbcTemplate);
    this.transactionTemplate = Preconditions.checkNotNull(transactionTemplate);
  }

  @Override
  public void add(final Set<ScheduledTask> newTasks) {
    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        insert(newTasks);
      }
    });
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

  @Override
  public void remove(final Query query) {
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

  @Override
  public void remove(final Set<String> taskIds) {
    transactionTemplate.execute(new TransactionCallbackWithoutResult() {
      @Override protected void doInTransactionWithoutResult(TransactionStatus status) {
        remove(restrictTaskIds(new WhereClauseBuilder(), taskIds));
      }
    });
  }

  private void remove(WhereClauseBuilder whereClauseBuilder) {
    StringBuilder sql = new StringBuilder("DELETE FROM task_state");
    jdbcTemplate.update(whereClauseBuilder.appendWhereClause(sql).toString(),
        whereClauseBuilder.parameters(), whereClauseBuilder.parameterTypes());
  }

  @Override
  public ImmutableSet<TaskState> mutate(final Query query, final Closure<TaskState> mutator) {
    return transactionTemplate.execute(new TransactionCallback<ImmutableSet<TaskState>>() {
      @Override public ImmutableSet<TaskState> doInTransaction(TransactionStatus status) {
        return update(query, mutator);
      }
    });
  }

  private ImmutableSet<TaskState> update(Query query, Closure<TaskState> mutator) {
    final ImmutableSortedSet<TaskState> taskStates = fetch(query);
    for (TaskState taskState : taskStates) {
      mutator.execute(taskState);
    }

    final Iterator<TaskState> tasks = taskStates.iterator();
    jdbcTemplate.batchUpdate("UPDATE task_state SET owner = ?, job_name = ?,"
                             + " job_key = ?, slave_host = ?, shard_id = ?, status = ?,"
                             + " scheduled_task = ? WHERE task_id = ?",
        new BatchPreparedStatementSetter() {
          @Override public void setValues(PreparedStatement preparedStatement, int batchItemIndex)
              throws SQLException {

            TaskState taskState = tasks.next();
            ScheduledTask scheduledTask = taskState.task;

            int col = prepareRow(preparedStatement, 1, scheduledTask);
            setTaskId(preparedStatement, col, scheduledTask);
          }

          @Override public int getBatchSize() {
            return taskStates.size();
          }
        });

    return taskStates;
  }

  @Override
  public ImmutableSortedSet<TaskState> fetch(final Query query) {
    return transactionTemplate.execute(new TransactionCallback<ImmutableSortedSet<TaskState>>() {
      @Override public ImmutableSortedSet<TaskState> doInTransaction(TransactionStatus status) {
        return ImmutableSortedSet.copyOf(Query.SORT_BY_TASK_ID, query(query));
      }
    });
  }

  @Override
  public Set<String> fetchIds(final Query query) {
    return transactionTemplate.execute(new TransactionCallback<Set<String>>() {
      @Override public Set<String> doInTransaction(TransactionStatus status) {
        return ImmutableSet.copyOf(Iterables.transform(query(query), Tasks.STATE_TO_ID));
      }
    });
  }

  private static boolean isEmpty(@Nullable Collection<?> items) {
    return items == null || items.isEmpty();
  }

  private Iterable<TaskState> query(Query query) {
    StringBuilder sqlBuilder = new StringBuilder("SELECT scheduled_task FROM task_state");

    WhereClauseBuilder whereClauseBuilder = createWhereClause(query);
    List<TaskState> results = jdbcTemplate.query(
        whereClauseBuilder.appendWhereClause(sqlBuilder).toString(),
        whereClauseBuilder.parameters(),
        whereClauseBuilder.parameterTypes(),
        new RowMapper<TaskState>() {
          @Override public TaskState mapRow(ResultSet resultSet, int rowIndex)
              throws SQLException {

            ScheduledTask scheduledTask;
            try {
              scheduledTask = ThriftBinaryCodec.decode(ScheduledTask.class, resultSet.getBytes(1));
            } catch (CodingException e) {
              throw new SQLException("Problem decoding ScheduledTask", e);
            }

            return new TaskState(scheduledTask);
          }
        });
    return query.hasPostFilter() ? Iterables.filter(results, query.postFilter()) : results;
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

  private WhereClauseBuilder createWhereClause(Query query) {
    // TODO(jsirois): investigate using:
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
    if (!isEmpty(taskQuery.getTaskIds())) {
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
      byte[] structBytes;
      try {
        structBytes = ThriftBinaryCodec.encode(struct);
      } catch (CodingException e) {
        throw new SQLException("Unexpected exception serializing a ScheduledTask struct", e);
      }
      preparedStatement.setBytes(col, structBytes);
    }
  }

}
