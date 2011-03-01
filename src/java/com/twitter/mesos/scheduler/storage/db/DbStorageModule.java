package com.twitter.mesos.scheduler.storage.db;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import com.twitter.common.application.http.HttpServletConfig;
import com.twitter.common.application.http.Registration;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.scheduler.SchedulerMain.TwitterSchedulerOptions;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.StorageRoles;
import org.h2.server.web.WebServlet;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import javax.sql.DataSource;

/**
 * Provides bindings for db based scheduler storage.
 *
 * @author John Sirois
 */
public class DbStorageModule extends AbstractModule {

  private final TwitterSchedulerOptions options;
  private final Role storageRole;

  @Inject
  public DbStorageModule(TwitterSchedulerOptions options, Role storageRole) {
    this.options = Preconditions.checkNotNull(options);
    this.storageRole = Preconditions.checkNotNull(storageRole);
  }

  @Override
  protected void configure() {
    bind(Storage.class).annotatedWith(StorageRoles.forRole(storageRole))
        .to(DbStorage.class).in(Singleton.class);

    // TODO(John Sirois): reconsider exposing the db in this way - obvious danger here
    ImmutableMap<String, String> initParams = ImmutableMap.of("webAllowOthers", "true");
    Registration.registerServlet(binder(),
        new HttpServletConfig("/scheduler/storage", WebServlet.class, initParams, true));
  }

  @Provides
  @Singleton
  DataSource provideDataSource() throws PropertyVetoException, IOException {
    File dbFilePath = new File(options.dbFilePath,
        String.format("h2-v%d", DbStorage.STORAGE_SYSTEM_VERSION));
    Files.createParentDirs(dbFilePath);

    ComboPooledDataSource dataSource = new ComboPooledDataSource();
    dataSource.setDriverClass(org.h2.Driver.class.getName());
    dataSource.setJdbcUrl(String.format("jdbc:h2:file:%s;AUTO_SERVER=TRUE;CACHE_SIZE=%d",
        dbFilePath.getPath(), options.dbCacheSize.as(Data.Kb)));

    // We're exposing the H2 web ui, so at least make the user/pass non-default.
    dataSource.setUser("scheduler");
    dataSource.setPassword("ep1nephrin3");

    // Consider accepting/setting connection pooling here.
    // c3p0 defaults of start=3,min=3,max=15,acquire=3 etc... seem fine for now

    return dataSource;
  }

  @Provides
  @Singleton
  JdbcTemplate provideJdbcTemplate(DataSource dataSource) {
    return new JdbcTemplate(dataSource);
  }

  @Provides
  @Singleton
  TransactionTemplate provideTransactionTemplate(DataSource dataSource) {
    return new TransactionTemplate(new DataSourceTransactionManager(dataSource));
  }
}
