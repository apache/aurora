package com.twitter.mesos.scheduler.storage.db;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import org.h2.server.web.WebServlet;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.twitter.common.application.http.HttpServletConfig;
import com.twitter.common.application.http.Registration;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.CanWrite;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.IsDirectory;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.StorageRoles;

/**
 * Provides bindings for db based scheduler storage.
 *
 * @author John Sirois
 */
public class DbStorageModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(DbStorageModule.class.getName());

  @Exists
  @CanRead
  @CanWrite
  @IsDirectory
  @CmdLine(name = "scheduler_db_file_path",
          help ="The path of the H2 db files.")
  private static final Arg<File> dbFilePath = Arg.create(new File("/tmp/mesos_scheduler_db"));

  @CmdLine(name = "scheduler_db_cache_size",
          help ="The size to use for the H2 in-memory db cache.")
  private static final Arg<Amount<Long, Data>> dbCacheSize = Arg.create(Amount.of(128L, Data.Mb));

  private final Role storageRole;

  @Inject
  public DbStorageModule(Role storageRole) {
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
    File dbFilePath = new File(DbStorageModule.dbFilePath.get(),
        String.format("h2-v%d", DbStorage.STORAGE_SYSTEM_VERSION));
    Files.createParentDirs(dbFilePath);
    LOG.info("Using db storage path: " + dbFilePath);

    ComboPooledDataSource dataSource = new ComboPooledDataSource();
    dataSource.setDriverClass(org.h2.Driver.class.getName());
    dataSource.setJdbcUrl(String.format("jdbc:h2:file:%s;AUTO_SERVER=TRUE;CACHE_SIZE=%d",
        dbFilePath.getPath(), dbCacheSize.get().as(Data.Kb)));

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
