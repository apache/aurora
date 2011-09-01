package com.twitter.mesos.scheduler.storage.db;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.logging.Logger;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateBinder;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.args.constraints.CanRead;
import com.twitter.common.args.constraints.CanWrite;
import com.twitter.common.args.constraints.Exists;
import com.twitter.common.args.constraints.IsDirectory;
import com.twitter.common.args.constraints.NotNull;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.quantity.Amount;
import com.twitter.common.quantity.Data;
import com.twitter.mesos.scheduler.db.DbUtil;
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
  private static final Arg<Amount<Long, Data>> dbCacheSize = Arg.create(Amount.of(256L, Data.MB));

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
    Preconditions.checkNotNull(binder);

    bindStorage(binder, Key.get(Storage.class), Closures.<PrivateBinder>noop());
  }

  /**
   * Binds the database {@link Storage} engine to the requested key.
   *
   * @param binder a guice binder to bind storage with
   * @param key the binding annotation key to bind DbStorage under.
   * @param bindAdditional a closure that can bind (and expose) additional items from the private
   *     binding scope.
   */
  public static void bind(Binder binder, Class<? extends Annotation> key,
      Closure<PrivateBinder> bindAdditional) {

    Preconditions.checkNotNull(binder);
    Preconditions.checkNotNull(key);
    Preconditions.checkNotNull(bindAdditional);

    bindStorage(binder, Key.get(Storage.class, key), bindAdditional);
  }

  private static void bindStorage(Binder binder, Key<Storage> key,
      Closure<PrivateBinder> bindAdditional) {

    binder.install(new DbStorageModule(key, bindAdditional));

    if (exposeDbAdmin.get()) {
      DbUtil.bindAdminInterface(binder, "/scheduler/storage");
    }
  }

  private final Key<Storage> storageKey;
  private Closure<PrivateBinder> bindAdditional;

  private DbStorageModule(Key<Storage> storageKey, Closure<PrivateBinder> bindAdditional) {
    this.storageKey = storageKey;
    this.bindAdditional = bindAdditional;
  }

  @Override
  protected void configure() {
    bind(storageKey).to(DbStorage.class);
    bind(DbStorage.class).in(Singleton.class);
    expose(storageKey);

    // TODO(John Sirois): Consider switching to inMemory(...) when we are successfully running on
    // the mesos-core log.
    DbUtil.fileSystem(dbFilePath.get(), "h2-v1", dbCacheSize.get())
        .secured("scheduler", "ep1nephrin3")
        .bind(binder());

    bindAdditional.execute(binder());
  }
}
