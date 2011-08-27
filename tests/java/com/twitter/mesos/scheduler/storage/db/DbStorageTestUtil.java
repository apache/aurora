package com.twitter.mesos.scheduler.storage.db;

import java.sql.SQLException;

import com.google.common.testing.TearDownAccepter;

import com.twitter.mesos.scheduler.db.testing.DbTestUtil;
import com.twitter.mesos.scheduler.db.testing.DbTestUtil.DbAccess;

/**
 * Provides utility methods for testing against DbStorage H2 databases.
 *
 * @author John Sirois
 */
public final class DbStorageTestUtil {

  /**
   * Sets up a DbStorage against a new in-memory database with empty tables.  Also props up the H2
   * web interface at http://localhost:8082 so the test database can be browsed at connection url:
   * jdbc:h2:mem:testdb.
   *
   * @param tearDownAccepter The {@code TearDownAccepter} for the test
   * @return A new DbStorage coupled to a fresh in-memory database
   * @throws SQLException if there is a problem setting up the fresh in-memory database
   */
  public static DbStorage setupStorage(TearDownAccepter tearDownAccepter) throws SQLException {
    DbAccess dbAccess = DbTestUtil.setupStorage(tearDownAccepter);
    return new DbStorage(dbAccess.jdbcTemplate, dbAccess.transactionTemplate);
  }

  private DbStorageTestUtil() {
    // utility
  }
}
