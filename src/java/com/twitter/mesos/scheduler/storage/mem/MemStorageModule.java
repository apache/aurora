package com.twitter.mesos.scheduler.storage.mem;

import com.google.common.base.Preconditions;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateBinder;
import com.google.inject.PrivateModule;
import com.google.inject.Singleton;

import com.twitter.common.base.Closure;
import com.twitter.common.base.Closures;
import com.twitter.common.inject.Bindings.KeyFactory;
import com.twitter.mesos.scheduler.storage.JobStore;
import com.twitter.mesos.scheduler.storage.QuotaStore;
import com.twitter.mesos.scheduler.storage.SchedulerStore;
import com.twitter.mesos.scheduler.storage.Storage;
import com.twitter.mesos.scheduler.storage.Storage.Volatile;
import com.twitter.mesos.scheduler.storage.TaskStore;
import com.twitter.mesos.scheduler.storage.UpdateStore;

/**
 * Binding module for an in-memory storage system.
 */
public final class MemStorageModule extends PrivateModule {

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
   * @param keyFactory bindng key factory to associate MemStorage with.
   * @param bindAdditional a closure that can bind (and expose) additional items from the private
   *     binding scope.
   */
  public static void bind(
      Binder binder,
      KeyFactory keyFactory,
      Closure<PrivateBinder> bindAdditional) {

    Preconditions.checkNotNull(binder);
    Preconditions.checkNotNull(keyFactory);
    Preconditions.checkNotNull(bindAdditional);

    bindStorage(binder, keyFactory.create(Storage.class), bindAdditional);
  }

  private static void bindStorage(Binder binder, Key<Storage> key,
      Closure<PrivateBinder> bindAdditional) {

    binder.install(new MemStorageModule(key, bindAdditional));
  }

  private final Key<Storage> storageKey;
  private Closure<PrivateBinder> bindAdditional;

  private MemStorageModule(Key<Storage> storageKey, Closure<PrivateBinder> bindAdditional) {
    this.storageKey = storageKey;
    this.bindAdditional = bindAdditional;
  }

  private <T> void bindStore(Class<T> binding, Class<? extends T> impl) {
    bind(binding).to(impl);
    bind(impl).in(Singleton.class);
  }

  @Override
  protected void configure() {
    bind(storageKey).to(MemStorage.class);
    Key<Storage> exposedMemStorageKey = Key.get(Storage.class, Volatile.class);
    bind(exposedMemStorageKey).to(MemStorage.class);
    expose(exposedMemStorageKey);
    bind(MemStorage.class).in(Singleton.class);

    bindStore(SchedulerStore.Mutable.class, MemSchedulerStore.class);
    bindStore(JobStore.Mutable.class, MemJobStore.class);
    bindStore(TaskStore.Mutable.class, MemTaskStore.class);
    bindStore(UpdateStore.Mutable.class, MemUpdateStore.class);
    bindStore(QuotaStore.Mutable.class, MemQuotaStore.class);

    expose(storageKey);
    bindAdditional.execute(binder());
  }
}
