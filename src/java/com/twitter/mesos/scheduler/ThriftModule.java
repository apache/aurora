package com.twitter.mesos.scheduler;

import com.google.common.base.Preconditions;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Singleton;

import com.twitter.common.thrift.ThriftServer;
import com.twitter.mesos.gen.MesosSchedulerManager;
import com.twitter.mesos.gen.MesosSchedulerManager.Iface;
import com.twitter.mesos.scheduler.migrations.SchemaMigrationDelegate;

/**
 * Binds components needed to serve the scheduler thrift interface.
 *
 * @author John Sirois
 */
abstract class ThriftModule extends AbstractModule {

  /**
   * Binds a {@link ThriftServer} and a {@link MesosSchedulerManager.Iface} that sanitizes thrift
   * inputs before handing requests off to a {@link SchemaMigrationDelegate}.  This binding is
   * appropriate fo deployments that must be able to handle both old and new thrift request data in
   * a thrift schema migration.
   *
   * <p>The contract for sanitizers is that they implement {@link MesosSchedulerManager.Iface} and
   * accept a {@link SchemaMigrationDelegate} annotated {@link MesosSchedulerManager.Iface} instance
   * to delegate sanitized requests to.
   *
   * @param binder a guice binder to thrift components with
   */
  public static void bindWithSchemaMigrator(Binder binder) {
    binder.install(new AbstractModule() {
      @Override protected void configure() {
        Key<MesosSchedulerManager.Iface> migrationDelegateKey =
            Key.get(MesosSchedulerManager.Iface.class, SchemaMigrationDelegate.class);
        requireBinding(migrationDelegateKey);
        bind(migrationDelegateKey).to(SchedulerThriftInterface.class).in(Singleton.class);
      }
    });

    // A volatile binding for the current sanitizer to apply to thrift structs.
    // A new or refactored sanitizer will need to be bound for each thrift schema change
    // requiring migration
    // v0_v1 => bindThriftEndpoint(binder, OwnerSanitizingSchedulerManager.class);
    throw new UnsupportedOperationException("No migrations present.");
  }

  /**
   * Binds a {@link ThriftServer} and a {@link MesosSchedulerManager.Iface} to handle requests with.
   *
   * @param binder a guice binder to thrift components with
   */
  public static void bind(Binder binder) {
    bindThriftEndpoint(binder, SchedulerThriftInterface.class);
  }

  private static void bindThriftEndpoint(Binder binder,
      Class<? extends MesosSchedulerManager.Iface> thriftEndpointClass) {
    binder.install(new ThriftModule(thriftEndpointClass) {});
  }

  private final Class<? extends MesosSchedulerManager.Iface> thriftEndpointClass;

  private ThriftModule(Class<? extends Iface> thriftEndpointClass) {
    this.thriftEndpointClass = Preconditions.checkNotNull(thriftEndpointClass);
  }

  @Override
  protected void configure() {
    bind(MesosSchedulerManager.Iface.class).to(thriftEndpointClass).in(Singleton.class);
    bind(ThriftServer.class).to(SchedulerThriftServer.class).in(Singleton.class);
  }
}
