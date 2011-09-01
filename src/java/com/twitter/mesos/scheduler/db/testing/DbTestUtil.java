package com.twitter.mesos.scheduler.db.testing;

import java.io.IOException;
import java.sql.SQLException;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.testing.TearDown;
import com.google.common.testing.TearDownAccepter;

import org.h2.tools.Server;

import com.twitter.common.base.MorePreconditions;
import com.twitter.common.testing.TearDownRegistry;
import com.twitter.mesos.scheduler.db.DbUtil;
import com.twitter.mesos.scheduler.db.DbUtil.DbAccess;

/**
 * Provides utility methods for testing against H2 databases.
 *
 * @author John Sirois
 */
public final class DbTestUtil {

  private static final Logger LOG = Logger.getLogger(DbTestUtil.class.getName());

  /**
   * Sets up a DbStorage against a new in-memory database with empty tables.  Also props up the H2
   * web interface so the test database can be browsed at connection url: jdbc:h2:mem:testdb.
   *
   * @param tearDownAccepter The {@code TearDownAccepter} for the test
   * @return database access objects coupled to a fresh in-memory database
   * @throws SQLException if there is a problem setting up the fresh in-memory database
   */
  public static DbAccess setupStorage(TearDownAccepter tearDownAccepter) throws SQLException {
    return setupStorage(tearDownAccepter, "testdb");
  }

  /**
   * Sets up a DbStorage against a new in-memory database with empty tables.  Also props up the H2
   * web interface so the test database can be browsed at connection url: jdbc:h2:mem:testdb.
   *
   * @param tearDownAccepter The {@code TearDownAccepter} for the test
   * @param dbName The name to use for the database
   * @return database access objects coupled to a fresh in-memory database
   * @throws SQLException if there is a problem setting up the fresh in-memory database
   */
  public static DbAccess setupStorage(TearDownAccepter tearDownAccepter, String dbName)
      throws SQLException {

    Preconditions.checkNotNull(tearDownAccepter);
    MorePreconditions.checkNotBlank(dbName);

    // Prop up a web console at: http://localhost:XXX, allows connections to jdbc:h2:mem:[dbName]
    final Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "0").start();
    tearDownAccepter.addTearDown(new TearDown() {
      @Override public void tearDown() {
        webServer.stop();
      }
    });
    LOG.info("Test db console for jdbc:h2:mem:" + dbName + " available at: " + webServer.getURL());

    try {
      return DbUtil.inMemory(dbName).build(new TearDownRegistry(tearDownAccepter));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected problem opening in-memory database", e);
    }
  }

  private DbTestUtil() {
    // utility
  }
}
