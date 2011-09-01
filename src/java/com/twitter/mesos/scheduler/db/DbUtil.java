package com.twitter.mesos.scheduler.db;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.logging.Logger;

import javax.annotation.Nullable;

import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.h2.server.web.WebServlet;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.twitter.common.application.ActionRegistry;
import com.twitter.common.application.ShutdownStage;
import com.twitter.common.application.http.HttpServletConfig;
import com.twitter.common.application.http.Registration;
import com.twitter.common.base.Command;
import com.twitter.common.base.MorePreconditions;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;


/**
 * Utilities for dealing with database operations.
 *
 * @author John Sirois
 */
public final class DbUtil {

  private static final Logger LOG = Logger.getLogger(DbUtil.class.getName());

  private DbUtil() {
    // utility
  }

  /**
   * Executes the sql defined in a resource file against the database covered by jdbcTemplate.
   *
   * @param jdbcTemplate The jdbc template object to execute database operation against.
   * @param sqlResource A handle to the resource contianing the sql to execute.
   * @param logSql {@code true} to log the applied sql
   * @throws IllegalArgumentException if the given sql resource does not exist
   */
  public static void executeSql(JdbcTemplate jdbcTemplate, ClassPathResource sqlResource,
      boolean logSql) {

    URL sqlResourceUrl = getResourceURL(sqlResource);
    jdbcTemplate.execute(String.format("RUNSCRIPT FROM '%s'", sqlResourceUrl));
    if (logSql) {
      try {
        LOG.info(Resources.toString(sqlResourceUrl, Charsets.UTF_8));
      } catch (IOException e) {
        LOG.warning("Failed to log sql that was successfully applied to db from: " + sqlResourceUrl);
      }
    }
  }

  private static URL getResourceURL(ClassPathResource resource) {
    Preconditions.checkArgument(resource.exists());

    try {
      return resource.getURL();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Unexpected problem obtaining URL for existing resource: " + resource);
    }
  }

  /**
   * A simple struct that groups templates used for database access.
   */
  public static class DbAccess {
    public final TransactionTemplate transactionTemplate;
    public final JdbcTemplate jdbcTemplate;

    private DbAccess(TransactionTemplate transactionTemplate, JdbcTemplate jdbcTemplate) {
      this.transactionTemplate = transactionTemplate;
      this.jdbcTemplate = jdbcTemplate;
    }
  }

  /**
   * Builds an in-process database.
   *
   * <p>TODO(John Sirois): Consider adding builder support for setting up connection pooling - c3p0
   * defaults of start=3,min=3,max=15,acquire=3 etc... seem fine for now.
   */
  public static class Builder {
    private final String dbName;
    private final File dbPath;
    private final Amount<Long, Data> cacheSize;

    private String username;
    private String password;

    private Builder(String dbName, @Nullable File dbPath, @Nullable Amount<Long, Data> cacheSize) {
      this.dbName = MorePreconditions.checkNotBlank(dbName);
      this.dbPath = dbPath;
      this.cacheSize = cacheSize;
    }

    /**
     * Secures the in-process database.
     *
     * @param username The username to secure database access with.
     * @param password The password to secure database access with.
     * @return This builder for chained calls.
     */
    public Builder secured(String username, String password) {
      this.username = MorePreconditions.checkNotBlank(username);
      this.password = MorePreconditions.checkNotBlank(password);
      return this;
    }

    /**
     * Creates the in-process database and starts it.
     *
     * @param shutdownRegistry An action registry to register database shutdown hooks with.
     * @return A {@code DbAccess} struct with templates that can be used to access the in-process
     *     database.
     * @throws IOException If there is a problem starting the in-process database.
     */
    public DbAccess build(ActionRegistry shutdownRegistry) throws IOException {
      Preconditions.checkNotNull(shutdownRegistry);

      final ComboPooledDataSource dataSource = new ComboPooledDataSource();
      final JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
      shutdownRegistry.addAction(new Command() {
        @Override public void execute() throws RuntimeException {
          // Ensure a prompt shutdown of active connections
          jdbcTemplate.execute("SHUTDOWN");

          dataSource.close();
        }
      });

      try {
        dataSource.setDriverClass(org.h2.Driver.class.getName());
      } catch (PropertyVetoException e) {
        throw new IllegalStateException(e);
      }

      String jdbcUrl = createJdbcUrl();
      dataSource.setJdbcUrl(jdbcUrl);
      LOG.info("Db available at: " + jdbcUrl);

      if (username != null) {
        dataSource.setUser(username);
        dataSource.setPassword(password);
      }

      return new DbAccess(new TransactionTemplate(new DataSourceTransactionManager(dataSource)),
          jdbcTemplate);
    }

    private String createJdbcUrl() throws IOException {
      if (dbPath != null) {
        File dbFilePath = new File(dbPath, dbName);
        Files.createParentDirs(dbFilePath);
        return String.format("jdbc:h2:file:%s;AUTO_SERVER=TRUE;CACHE_SIZE=%d",
            dbFilePath.getPath(), cacheSize.as(Data.KB));
      } else {
        return "jdbc:h2:mem:" + dbName;
      }
    }

    /**
     * Binds a {@link TransactionTemplate} and a {@link JdbcTemplate} for the database specified by
     * this builder in {@link Singleton} scope.  Requires a {@link ShutdownStage}
     * {@link ActionRegistry} be bound.
     *
     * @param binder The binder to bind the database templates against.
     */
    public void bind(Binder binder) {
      Preconditions.checkNotNull(binder);
      binder.install(new AbstractModule() {
        @Override protected void configure() {
          requireBinding(Key.get(ActionRegistry.class, ShutdownStage.class));
        }

        @Provides @Singleton
        DbAccess provideDataSource(@ShutdownStage ActionRegistry actionRegistry)
            throws IOException {
          return Builder.this.build(actionRegistry);
        }

        @Provides @Singleton
        JdbcTemplate provideJdbcTemplate(DbAccess dbAccess) {
          return dbAccess.jdbcTemplate;
        }

        @Provides @Singleton
        TransactionTemplate provideTransactionTemplate(DbAccess dbAccess) {
          return dbAccess.transactionTemplate;
        }
      });
    }
  }

  /**
   * Creates a builder that can be used to create an pure in-memory in-process database with no
   * persistent storage.
   *
   * @param dbName A unique name for the database in this process.
   * @return A builder that can be used to further specify database parameters and to finally create
   *     the database accessors.
   */
  public static Builder inMemory(String dbName) {
    return new Builder(dbName, null, null);
  }

  /**
   * Creates a builder that can be used to create an in-process database with a filesystem based
   * backing store.
   *
   * @param dbPath The path to store database files under.
   * @param dbName A unique name for the database files.
   * @param cacheSize The amount of data to try to keep in memory for fast queries.
   * @return A builder that can be used to further specify database parameters and to finally create
   *     the database accessors.
   */
  public static Builder fileSystem(File dbPath, String dbName, Amount<Long, Data> cacheSize) {
    Preconditions.checkNotNull(dbPath);
    Preconditions.checkNotNull(cacheSize);
    Preconditions.checkArgument(cacheSize.as(Data.KB) > 0,
        "Must have at least 1KB of cache, given: %s", cacheSize);

    return new Builder(dbName, dbPath, cacheSize);
  }

  /**
   * Binds an web ui for database administration at the given path.
   *
   * @param binder The binder to bind the servlet against.
   * @param path The path to mount the servlet at.
   */
  public static void bindAdminInterface(Binder binder, String path) {
    Preconditions.checkNotNull(binder);
    MorePreconditions.checkNotBlank(path);

    ImmutableMap<String, String> initParams = ImmutableMap.of("webAllowOthers", "true");
    Registration.registerServlet(binder,
        new HttpServletConfig(path, WebServlet.class, initParams, true));
  }
}
