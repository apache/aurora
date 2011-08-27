package com.twitter.mesos.scheduler.storage.db;

import java.beans.PropertyVetoException;
import java.io.File;
import java.io.IOException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.google.inject.Binder;
import com.google.inject.PrivateModule;
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
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.scheduler.storage.Storage;

/**
 * Provides bindings for db based scheduler storage.
 *
 * @author John Sirois
 */
public class DbStorageModule extends PrivateModule {

  @NotNull
  @Exists
  @CanRead
  @CanWrite
  @IsDirectory
  @CmdLine(name = "scheduler_db_file_path",
          help ="The path of the H2 db files.")
  private static final Arg<File> dbFilePath = Arg.create();

  @CmdLine(name = "scheduler_db_cache_size",
          help ="The size to use for the H2 in-memory db cache.")
  private static final Arg<Amount<Long, Data>> dbCacheSize = Arg.create(Amount.of(256L, Data.Mb));

  // TODO(John Sirois): reconsider exposing the db by default - obvious danger here
  @CmdLine(name = "scheduler_db_admin_interface",
          help ="Turns on a web interface to the storage db - use with caution")
  private static final Arg<Boolean> exposeDbAdmin = Arg.create(true);

  private static final Logger LOG = Logger.getLogger(DbStorageModule.class.getName());

  /**
   * Binds the database {@link Storage} engine.
   *
   * @param binder a guice binder to bind storage with
   */
  public static void bind(Binder binder) {
    binder.install(new DbStorageModule());
    installAdminInterface(binder);
  }

  private static void installAdminInterface(Binder binder) {
    if (exposeDbAdmin.get()) {
      ImmutableMap<String, String> initParams = ImmutableMap.of("webAllowOthers", "true");
      Registration.registerServlet(binder,
          new HttpServletConfig("/scheduler/storage", WebServlet.class, initParams, true));
    }
  }

  private DbStorageModule() {
    // must bind(...)
  }

  @Override
  protected void configure() {
    bind(Storage.class).to(DbStorage.class).in(Singleton.class);
    expose(Storage.class);
  }

  @Provides
  @Singleton
  DataSource provideDataSource() throws PropertyVetoException, IOException {
    File dbFilePath = new File(DbStorageModule.dbFilePath.get(), "h2-v1");
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
