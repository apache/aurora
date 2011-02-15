package com.twitter.mesos.scheduler.storage.db;

import com.google.common.base.Preconditions;
import com.google.common.testing.TearDown;
import com.google.common.testing.TearDownAccepter;
import org.h2.tools.Server;
import org.springframework.core.io.ClassRelativeResourceLoader;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.SQLException;

/**
 * Provides utility methods for testing against H2 databases.
 *
 * @author jsirois
 */
final class DbStorageTestUtil {

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

    Preconditions.checkNotNull(tearDownAccepter);

    // Prop up a web console at: http://localhost:8082, allows connections to jdbc:h2:mem:testdb
    final Server webServer = Server.createWebServer("-webAllowOthers").start();
    tearDownAccepter.addTearDown(new TearDown() {
      @Override public void tearDown() {
        webServer.stop();
      }
    });

    final EmbeddedDatabase embeddedDatabase =
        new EmbeddedDatabaseBuilder()
            .setType(EmbeddedDatabaseType.H2)
            .build();
    tearDownAccepter.addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        embeddedDatabase.shutdown();
      }
    });

    JdbcTemplate jdbcTemplate = new JdbcTemplate(embeddedDatabase);
    TransactionTemplate transactionTemplate =
        new TransactionTemplate(new DataSourceTransactionManager(embeddedDatabase));
    return new DbStorage(jdbcTemplate, transactionTemplate);
  }

  private DbStorageTestUtil() {
    // utility
  }
}
