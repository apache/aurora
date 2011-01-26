package com.twitter.mesos.scheduler;

import com.google.common.testing.TearDown;
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
 * @author jsirois
 */
public class DbTaskStoreTest extends BaseTaskStoreTest<DbTaskStore> {

  @Override
  protected DbTaskStore createTaskStore() throws SQLException {
    // Prop up a web console at: http://localhost:8082, allows connections tojdbc:h2:mem:testdb
    final Server webServer = Server.createWebServer("-webAllowOthers").start();
    addTearDown(new TearDown() {
      @Override public void tearDown() {
        webServer.stop();
      }
    });

    final EmbeddedDatabase embeddedDatabase =
        new EmbeddedDatabaseBuilder(new ClassRelativeResourceLoader(DbTaskStore.class))
            .setType(EmbeddedDatabaseType.H2)
            .addScript("db-task-store-schema.sql")
            .build();
    addTearDown(new TearDown() {
      @Override public void tearDown() throws Exception {
        embeddedDatabase.shutdown();
      }
    });

    JdbcTemplate jdbcTemplate = new JdbcTemplate(embeddedDatabase);
    TransactionTemplate transactionTemplate =
        new TransactionTemplate(new DataSourceTransactionManager(embeddedDatabase));
    return new DbTaskStore(jdbcTemplate, transactionTemplate);
  }
}
