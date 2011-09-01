package com.twitter.mesos.scheduler.db.testing;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.common.testing.TearDown;
import com.google.common.testing.TearDownAccepter;

import org.h2.tools.Server;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.support.TransactionTemplate;

import com.twitter.common.base.MorePreconditions;

/**
 * Provides utility methods for testing against H2 databases.
 *
 * @author John Sirois
 */
public final class DbTestUtil {

  private static final Logger LOG = Logger.getLogger(DbTestUtil.class.getName());

  /**
   * A collection of accessors for tests to use against an in-memory database.
   */
  public static class DbAccess {
    public final URL adminInterface;
    public final TransactionTemplate transactionTemplate;
    public final JdbcTemplate jdbcTemplate;

    private DbAccess(URL adminInterface, TransactionTemplate transactionTemplate,
        JdbcTemplate jdbcTemplate) {
      this.adminInterface = adminInterface;
      this.transactionTemplate = transactionTemplate;
      this.jdbcTemplate = jdbcTemplate;
    }
  }

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

    // Prop up a web console at: http://localhost:XXX, allows connections to jdbc:h2:mem:testdb
    final Server webServer = Server.createWebServer("-webAllowOthers", "-webPort", "0").start();
    tearDownAccepter.addTearDown(new TearDown() {
      @Override public void tearDown() {
        webServer.stop();
      }
    });

    final EmbeddedDatabase embeddedDatabase =
        new EmbeddedDatabaseBuilder()
            .setType(EmbeddedDatabaseType.H2)
            .setName(dbName)
            .build();
    tearDownAccepter.addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        embeddedDatabase.shutdown();
      }
    });

    String adminUrl = webServer.getURL();
    LOG.info("Test db console for jdbc:h2:mem:" + dbName + " available at: " + adminUrl);
    try {
      return new DbAccess(new URL(adminUrl),
          new TransactionTemplate(new DataSourceTransactionManager(embeddedDatabase)),
          new JdbcTemplate(embeddedDatabase));
    } catch (MalformedURLException e) {
      throw new IllegalStateException(
          "Unexpected problem parsing the database admin URL: " + adminUrl, e);
    }
  }

  private DbTestUtil() {
    // utility
  }
}
