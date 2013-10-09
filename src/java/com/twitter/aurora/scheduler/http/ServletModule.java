/*
 * Copyright 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twitter.aurora.scheduler.http;

import java.util.Map;

import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.MediaType;
import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

import com.twitter.aurora.scheduler.quota.QuotaManager;
import com.twitter.aurora.scheduler.state.CronJobManager;
import com.twitter.aurora.scheduler.state.SchedulerCore;
import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;
import com.twitter.common.webassets.bootstrap.BootstrapModule;
import com.twitter.common.webassets.bootstrap.BootstrapModule.BootstrapVersion;
import com.twitter.common.webassets.jquery.JQueryModule;
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
    install(new BootstrapModule(BootstrapVersion.VERSION_2_3_2));

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
        bind(HttpStatsFilter.class).in(Singleton.class);
        filter("/scheduler*").through(HttpStatsFilter.class);
        bind(LeaderRedirectFilter.class).in(Singleton.class);
        registerJerseyEndpoint("/cron", Cron.class);
        registerJerseyEndpoint("/maintenance", Maintenance.class);
        registerJerseyEndpoint("/mname", Mname.class);
        registerJerseyEndpoint("/offers", Offers.class);
        registerJerseyEndpoint("/pendingtasks", PendingTasks.class);
        registerJerseyEndpoint("/quotas", Quotas.class);
        registerJerseyEndpoint(
            "/scheduler",
            SchedulerzHome.class,
            SchedulerzRole.class,
            SchedulerzJob.class);
        registerJerseyEndpoint("/slaves", Slaves.class);
        registerJerseyEndpoint("/structdump", StructDump.class);
        registerJerseyEndpoint("/utilization", Utilization.class);
      }
    });

    // Static assets.
    registerAsset("assets/util.js", "/js/util.js");
    registerAsset("assets/dictionary.js", "/js/dictionary.js");
    registerAsset("assets/images/viz.png", "/images/viz.png");
    registerAsset("assets/datatables/css/jquery.dataTables.css", "/css/jquery.dataTables.css");
    registerAsset("assets/datatables/images/back_disabled.png", "/images/back_disabled.png");
    registerAsset(
        "assets/datatables/images/back_enabled_hover.png",
        "/images/back_enabled_hover.png");
    registerAsset("assets/datatables/images/back_enabled.png", "/images/back_enabled.png");
    registerAsset(
        "assets/datatables/images/forward_disabled.png",
        "/images/forward_disabled.png");
    registerAsset(
        "assets/datatables/images/forward_enabled_hover.png",
        "/images/forward_enabled_hover.png");
    registerAsset(
        "assets/datatables/images/forward_enabled.png",
        "/images/forward_enabled.png");
    registerAsset(
        "assets/datatables/images/sort_asc_disabled.png",
        "/images/sort_asc_disabled.png");
    registerAsset("assets/datatables/images/sort_asc.png", "/images/sort_asc.png");
    registerAsset("assets/datatables/images/sort_both.png", "/images/sort_both.png");
    registerAsset(
        "assets/datatables/images/sort_desc_disabled.png",
        "/images/sort_desc_disabled.png");
    registerAsset("assets/datatables/images/sort_desc.png", "/images/sort_desc.png");
    registerAsset(
        "assets/datatables/js/jquery.dataTables.min.js",
        "/js/jquery.dataTables.min.js");
    registerAsset(
        "assets/datatables/js/dataTables.bootstrap.js",
        "/js/dataTables.bootstrap.js");
    registerAsset(
        "assets/datatables/js/dataTables.localstorage.js",
        "/js/dataTables.localstorage.js");
    registerAsset(
        "assets/datatables/js/dataTables.htmlNumberType.js",
        "/js/dataTables.htmlNumberType.js");

    registerAsset("assets/workflow/workflow.html", "/workflow/workflow.html");
    registerAsset("assets/workflow/tangle/BVTouchable.js", "/workflow/tangle/BVTouchable.js");
    registerAsset("assets/workflow/tangle/Tangle.js", "/workflow/tangle/Tangle.js");
    registerAsset("assets/workflow/tangle/TangleKit.css", "/workflow/tangle/TangleKit.css");
    registerAsset("assets/workflow/tangle/TangleKit.js", "/workflow/tangle/TangleKit.js");
    registerAsset("assets/workflow/tangle/mootools.js", "/workflow/tangle/mootools.js");
    registerAsset("assets/workflow/tangle/sprintf.js", "/workflow/tangle/sprintf.js");

    bind(LeaderRedirect.class).in(Singleton.class);
    LifecycleModule.bindStartupAction(binder(), RedirectMonitor.class);
  }

  private void registerAsset(String resourceLocation, String registerLocation) {
    MediaType mediaType;

    if (registerLocation.endsWith(".png")) {
      mediaType = MediaType.PNG;
    } else if (registerLocation.endsWith(".js")) {
      mediaType = MediaType.JAVASCRIPT_UTF_8;
    } else if (registerLocation.endsWith(".html")) {
      mediaType = MediaType.HTML_UTF_8;
    } else if (registerLocation.endsWith(".css")) {
      mediaType = MediaType.CSS_UTF_8;
    } else {
      throw new IllegalArgumentException("Could not determine media type for "
          + registerLocation);
    }

    Registration.registerHttpAsset(
      binder(),
      registerLocation,
      ServletModule.class,
      resourceLocation,
      mediaType.toString(),
      true);
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
