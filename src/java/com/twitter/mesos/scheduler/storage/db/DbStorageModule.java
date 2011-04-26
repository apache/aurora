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
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.multibindings.Multibinder;
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
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.ImmediateJobManager;
import com.twitter.mesos.scheduler.JobManager;
import com.twitter.mesos.scheduler.storage.DualStoreMigrator;
import com.twitter.mesos.scheduler.storage.DualStoreMigrator.DataMigrator;
import com.twitter.mesos.scheduler.storage.Migrator;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.StorageRole;
import com.twitter.mesos.scheduler.storage.StorageRole.Role;
import com.twitter.mesos.scheduler.storage.StorageRoles;
import com.twitter.mesos.scheduler.storage.db.migrations.v0_v1.OwnerMigrator;

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

  /**
   * Binds the {@link Role#Primary} database {@link Storage} engine.
   *
   * @param binder a guice binder to bind primary storage with
   */
  public static void bind(Binder binder) {
    binder.install(new DbStorageModule(Role.Primary, DbStorage.CURRENT_VERSION));
    installAdminInterface(binder);
  }

  /**
   * Binds the {@link Role#Primary} database {@link Storage} engine.  Also binds the previous
   * {@link Role#Legacy} database {@link Storage} engine and a {@link Migrator} to migrate data from
   * the legacy database to the primary database.
   *
   * @param binder a guice binder to bind primary storage, legacy storage and a migrator
   */
  public static void bindWithSchemaMigrator(Binder binder) {
    binder.install(new DbStorageModule(Role.Legacy, DbStorage.CURRENT_VERSION - 1));
    binder.install(new DbStorageModule(Role.Primary, DbStorage.CURRENT_VERSION) {
      @Override protected void configure() {
        super.configure();

        // DualStoreMigrator needs a binding for the Set of installed job managers
        Multibinder<JobManager> jobManagers = Multibinder.newSetBinder(binder(), JobManager.class);
        jobManagers.addBinding().to(CronJobManager.class);
        jobManagers.addBinding().to(ImmediateJobManager.class);

        // OwnerMigrator needs to be injected with the primary (target) JdbcTemplate and
        // TransactionTemplate so we install the migrator inside the Primary private module to avoid
        // exposing those bindings
        bind(DataMigrator.class).to(OwnerMigrator.class);

        DualStoreMigrator.bind(binder());
        expose(Migrator.class);
      }
    });
    installAdminInterface(binder);
  }

  private static void installAdminInterface(Binder binder) {
    if (exposeDbAdmin.get()) {
      binder.install(new AbstractModule() {
        @Override protected void configure() {
          ImmutableMap<String, String> initParams = ImmutableMap.of("webAllowOthers", "true");
          Registration.registerServlet(binder(),
              new HttpServletConfig("/scheduler/storage", WebServlet.class, initParams, true));
        }
      });
    }
  }

  private static final Logger LOG = Logger.getLogger(DbStorageModule.class.getName());

  private final Role storageRole;
  private final int version;

  private DbStorageModule(Role storageRole, int version) {
    this.storageRole = Preconditions.checkNotNull(storageRole);
    this.version = version;
  }

  @Override
  protected void configure() {
    bindConstant().annotatedWith(Version.class).to(version);

    StorageRole storageRole = StorageRoles.forRole(this.storageRole);
    Key<Storage> storageKey = Key.get(Storage.class, storageRole);
    Key<DbStorage> dbStorageKey = Key.get(DbStorage.class, storageRole);

    bind(storageKey).to(dbStorageKey);
    bind(dbStorageKey).to(DbStorage.class).in(Singleton.class);

    expose(storageKey);
    expose(dbStorageKey);
  }

  @Provides
  @Singleton
  DataSource provideDataSource() throws PropertyVetoException, IOException {
    File dbFilePath = new File(DbStorageModule.dbFilePath.get(),
        String.format("h2-v%d", version));
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
