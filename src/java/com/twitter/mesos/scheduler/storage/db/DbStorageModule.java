package com.twitter.mesos.scheduler.storage.db;

import java.lang.annotation.Annotation;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateBinder;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.mesos.scheduler.db.DbUtil;
import com.twitter.mesos.scheduler.storage.Storage;

/**
 * Provides bindings for db based scheduler storage.
 */
public class DbStorageModule extends PrivateModule {

  // TODO(John Sirois): reconsider exposing the db by default - obvious danger here
  @CmdLine(name = "scheduler_db_admin_interface",
          help ="Turns on a web interface to the storage db - use with caution")
  private static final Arg<Boolean> exposeDbAdmin = Arg.create(true);

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

    DbUtil.inMemory("h2-v1")
        .secured("scheduler", "ep1nephrin3")
        .bind(binder());

    bindAdditional.execute(binder());
  }
}
