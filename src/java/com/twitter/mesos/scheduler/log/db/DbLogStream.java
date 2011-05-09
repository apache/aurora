package com.twitter.mesos.scheduler.log.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Iterator;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Longs;
import com.google.inject.Inject;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import com.twitter.mesos.scheduler.db.DbUtil;
import com.twitter.mesos.scheduler.log.Log;
import com.twitter.mesos.scheduler.log.Log.Stream;

/**
 * A simple implementation of a {@code Log} backed by a database.  Only intended for testing.
 *
 * @author John Sirois
 */
public class DbLogStream implements Log, Stream {

  static class LongPosition implements Position {
    static final LongPosition BEGINNING = new LongPosition(0);

    private static final int LONG_BYTES = Long.SIZE / Byte.SIZE;

    private static long deserializeValue(byte[] value) {
      if (value == null) {
        throw new InvalidPositionException("null is not a valid position value");
      }
      try {
        return Longs.fromByteArray(value);
      } catch (IllegalArgumentException e) {
        throw new InvalidPositionException(
            String.format("value is not the right size, expected %d bytes, given %d: %s",
                LONG_BYTES, value.length, Hex.encodeHexString(value)),
            e);
      }
    }
    private final long position;

    LongPosition(byte[] value) {
      this(deserializeValue(value));
    }

    LongPosition(long position) {
      this.position = position;
    }

    @Override
    public byte[] identity() {
      return Longs.toByteArray(position);
    }

    @Override
    public int compareTo(Position other) {
      Preconditions.checkArgument(other instanceof LongPosition);
      return Long.signum(position - ((LongPosition) other).position);
    }

    @Override
    public boolean equals(Object other) {
      return other instanceof LongPosition && position == ((LongPosition) other).position;
    }

    @Override
    public String toString() {
      return "Position " + position;
    }
  }

  static class DbEntry implements Entry {
    private final int MAX_DUMP_BYTES = 32;

    private final LongPosition position;
    private final byte[] contents;

    DbEntry(LongPosition position, byte[] contents) {
      this.position = Preconditions.checkNotNull(position);
      this.contents = Preconditions.checkNotNull(contents);
    }

    @Override
    public LongPosition position() {
      return position;
    }

    @Override
    public byte[] contents() {
      return contents;
    }

    @Override
    public String toString() {
      String hex =
          Hex.encodeHexString(contents.length <= MAX_DUMP_BYTES
              ? contents : Arrays.copyOf(contents, MAX_DUMP_BYTES));
      return String.format("Entry @ %s %d bytes: %s", position, contents.length,
          StringUtils.abbreviate(hex, MAX_DUMP_BYTES * 2));
    }
  }

  private final TransactionTemplate transactionTemplate;
  private final JdbcTemplate jdbcTemplate;
  private boolean opened;

  @Inject
  public DbLogStream(TransactionTemplate transactionTemplate, JdbcTemplate jdbcTemplate) {
    this.transactionTemplate = Preconditions.checkNotNull(transactionTemplate);
    this.jdbcTemplate = Preconditions.checkNotNull(jdbcTemplate);
  }

  @Override
  public synchronized Stream open() {
    if (!opened) {
      opened = transactionTemplate.execute(new TransactionCallback<Boolean>() {
        @Override public Boolean doInTransaction(TransactionStatus status) {
          DbUtil.executeSql(
              jdbcTemplate, new ClassPathResource("db-log-stream-schema.sql", getClass()), false);
          return true;
        }
      });
    }
    return this;
  }

  @Override
  public Position position(byte[] value) throws InvalidPositionException {
    Preconditions.checkNotNull(value);
    return new LongPosition(value);
  }

  private static final String[] GENERATED_KEY = { "id" };

  @Override
  public LongPosition append(final byte[] contents) {
    Preconditions.checkNotNull(contents);
    return transactionTemplate.execute(new TransactionCallback<LongPosition>() {
      @Override public LongPosition doInTransaction(TransactionStatus status) {
        GeneratedKeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(new PreparedStatementCreator() {
          @Override public PreparedStatement createPreparedStatement(Connection connection)
              throws SQLException {
            PreparedStatement preparedStatement =
                connection.prepareStatement("INSERT INTO log_stream (contents) VALUES (?)",
                    GENERATED_KEY);
            preparedStatement.setBytes(1, contents);
            return preparedStatement;
          }
        }, keyHolder);
        return new LongPosition(keyHolder.getKey().longValue());
      }
    });
  }

  @Override
  public Iterator<Entry> readAfter(Position position) throws InvalidPositionException {
    Preconditions.checkNotNull(position);
    final LongPosition after = checkPosition(position);
    return transactionTemplate.execute(new TransactionCallback<Iterator<Entry>>() {
      @Override public Iterator<Entry> doInTransaction(TransactionStatus status) {
        return jdbcTemplate.query("SELECT id, contents FROM log_stream WHERE id > ?",
            new RowMapper<Entry>() {
              @Override public Entry mapRow(ResultSet rs, int rowNum) throws SQLException {
                return new DbEntry(new LongPosition(rs.getLong("id")), rs.getBytes("contents"));
              }
            }, after.position).iterator();
      }
    });
  }

  @Override
  public long truncateTo(Position position) throws InvalidPositionException {
    Preconditions.checkNotNull(position);
    final LongPosition to = checkPosition(position);
    return transactionTemplate.execute(new TransactionCallback<Integer>() {
      @Override public Integer doInTransaction(TransactionStatus status) {
        return jdbcTemplate.update("DELETE FROM log_stream WHERE id <= ?", to.position);
      }
    });
  }

  @Override
  public Position beginning() {
    return LongPosition.BEGINNING;
  }

  @Override
  public Position end() {
    return transactionTemplate.execute(new TransactionCallback<Position>() {
      @Override public Position doInTransaction(TransactionStatus status) {
        return new LongPosition(jdbcTemplate.queryForLong("SELECT MAX(id) FROM log_stream"));
      }
    });
  }

  private TransactionCallback<Long> fetchSize = new TransactionCallback<Long>() {
    @Override public Long doInTransaction(TransactionStatus status) {
      return jdbcTemplate.queryForLong("SELECT COUNT(id) FROM log_stream");
    }
  };

  @Override
  public long size() {
    return transactionTemplate.execute(fetchSize);
  }

  @Override
  public void close() {
    // noop
  }

  private LongPosition checkPosition(Position position) {
    if (!(position instanceof LongPosition)) {
      throw new InvalidPositionException("Cannot use a foreign position: " + position);
    }
    return (LongPosition) position;
  }
}
