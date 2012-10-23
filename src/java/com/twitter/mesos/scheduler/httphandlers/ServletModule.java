package com.twitter.mesos.scheduler.httphandlers;

import java.util.Map;

import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;
import com.twitter.common.webassets.bootstrap.BootstrapModule;
import com.twitter.common.webassets.jquery.JQueryModule;
import com.twitter.mesos.scheduler.ClusterName;
import com.twitter.mesos.scheduler.CronJobManager;
import com.twitter.mesos.scheduler.LeaderRedirect;
import com.twitter.mesos.scheduler.SchedulerCore;
import com.twitter.mesos.scheduler.quota.QuotaManager;
import com.twitter.thrift.ServiceInstance;

import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS;
import static com.sun.jersey.api.json.JSONConfiguration.FEATURE_POJO_MAPPING;

/**
 * Binding module for scheduler HTTP servlets.
 */
public class ServletModule extends AbstractModule {

  private static final Map<String, String> CONTAINER_PARAMS = ImmutableMap.of(
      FEATURE_POJO_MAPPING, Boolean.TRUE.toString(),
      PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName(),
      PROPERTY_CONTAINER_RESPONSE_FILTERS, GZIPContentEncodingFilter.class.getName());

  @Override
  protected void configure() {
    requireBinding(SchedulerCore.class);
    requireBinding(CronJobManager.class);
    requireBinding(Key.get(String.class, ClusterName.class));
    requireBinding(QuotaManager.class);

    install(new JQueryModule());
    install(new BootstrapModule());

    // Bindings required for the leader redirector.
    requireBinding(LocalServiceRegistry.class);
    requireBinding(Key.get(new TypeLiteral<DynamicHostSet<ServiceInstance>>() { }));
    Registration.registerServletFilter(binder(), GuiceFilter.class, "/*");
    install(new JerseyServletModule() {
      private void registerJerseyEndpoint(String indexPath, Class<?>... servlets) {
        filter(indexPath + "*").through(LeaderRedirectFilter.class);
        filter(indexPath + "*").through(GuiceContainer.class, CONTAINER_PARAMS);
        Registration.registerEndpoint(binder(), indexPath);
        for (Class<?> servlet : servlets) {
          bind(servlet);
        }
      }

      @Override protected void configureServlets() {
        bind(LeaderRedirectFilter.class).in(Singleton.class);
        registerJerseyEndpoint("/mname", Mname.class);
        registerJerseyEndpoint(
            "/scheduler",
            SchedulerzHome.class,
            SchedulerzRole.class,
            SchedulerzJob.class);
        registerJerseyEndpoint("/slaves", Slaves.class);
        registerJerseyEndpoint("/structdump", StructDump.class);
        registerJerseyEndpoint("/utilization", Utilization.class);
        registerJerseyEndpoint("/cron", Cron.class);
      }
    });

    // Static assets.
    Registration.registerHttpAsset(binder(), "/js/util.js", ServletModule.class,
        "assets/util.js", "application/javascript", true);
    Registration.registerHttpAsset(binder(), "/assets/thermos.png", ServletModule.class,
        "assets/thermos.png", "image/png", true);

    bind(LeaderRedirect.class).in(Singleton.class);
    LifecycleModule.bindStartupAction(binder(), RedirectMonitor.class);
  }

  static class RedirectMonitor implements ExceptionalCommand<MonitorException> {

    private final LeaderRedirect redirector;

    @Inject
    RedirectMonitor(LeaderRedirect redirector) {
      this.redirector = Preconditions.checkNotNull(redirector);
    }

    @Override public void execute() throws MonitorException {
      redirector.monitor();
    }
  }
}
