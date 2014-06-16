/**
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
package org.apache.aurora.scheduler.http;

import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.MediaType;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.application.modules.LocalServiceRegistry;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.net.pool.DynamicHostSet;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;
import com.twitter.thrift.ServiceInstance;

import org.apache.aurora.scheduler.cron.CronJobManager;
import org.apache.aurora.scheduler.quota.QuotaManager;
import org.apache.aurora.scheduler.state.SchedulerCore;
import org.apache.aurora.scheduler.storage.entities.IServerInfo;
import org.mortbay.servlet.GzipFilter;

import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS;
import static com.sun.jersey.api.json.JSONConfiguration.FEATURE_POJO_MAPPING;

/**
 * Binding module for scheduler HTTP servlets.
 */
public class ServletModule extends AbstractModule {

  @CmdLine(name = "enable_cors_support", help = "Enable CORS support for thrift end points.")
  private static final Arg<Boolean> ENABLE_CORS_SUPPORT = Arg.create(false);

  // More info on CORS can be found at http://enable-cors.org/index.html
  @CmdLine(name = "enable_cors_for",
      help = "List of domains for which CORS support should be enabled.")
  private static final Arg<String> ENABLE_CORS_FOR = Arg.create("*");

  private static final Map<String, String> CONTAINER_PARAMS = ImmutableMap.of(
      FEATURE_POJO_MAPPING, Boolean.TRUE.toString(),
      PROPERTY_CONTAINER_REQUEST_FILTERS, GZIPContentEncodingFilter.class.getName(),
      PROPERTY_CONTAINER_RESPONSE_FILTERS, GZIPContentEncodingFilter.class.getName());

  @Override
  protected void configure() {
    requireBinding(SchedulerCore.class);
    requireBinding(CronJobManager.class);
    requireBinding(IServerInfo.class);
    requireBinding(QuotaManager.class);

    // Register /api end point
    Registration.registerServlet(binder(), "/api", SchedulerAPIServlet.class, true);

    // NOTE: GzipFilter is applied only to /api instead of globally because the Jersey-managed
    // servlets have a conflicting filter applied to them.
    Registration.registerServletFilter(binder(), GzipFilter.class, "/api/*");

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

      @Override
      protected void configureServlets() {
        bind(HttpStatsFilter.class).in(Singleton.class);
        filter("/scheduler*").through(HttpStatsFilter.class);
        bind(LeaderRedirectFilter.class).in(Singleton.class);
        filter("/scheduler").through(LeaderRedirectFilter.class);

        // Add CORS support for all /api end points.
        if (ENABLE_CORS_SUPPORT.get()) {
          bind(CorsFilter.class).toInstance(new CorsFilter(ENABLE_CORS_FOR.get()));
          filter("/api*").through(CorsFilter.class);
        }

        registerJerseyEndpoint("/cron", Cron.class);
        registerJerseyEndpoint("/locks", Locks.class);
        registerJerseyEndpoint("/maintenance", Maintenance.class);
        registerJerseyEndpoint("/mname", Mname.class);
        registerJerseyEndpoint("/offers", Offers.class);
        registerJerseyEndpoint("/pendingtasks", PendingTasks.class);
        registerJerseyEndpoint("/quotas", Quotas.class);
        registerJerseyEndpoint("/slaves", Slaves.class);
        registerJerseyEndpoint("/structdump", StructDump.class);
        registerJerseyEndpoint("/utilization", Utilization.class);
      }
    });

    // Static assets.
    registerJQueryAssets();
    registerBootstrapAssets();

    registerAsset("assets/images/viz.png", "/images/viz.png");
    registerAsset("assets/images/aurora.png", "/images/aurora.png");
    registerAsset("assets/images/aurora_logo.png", "/images/aurora_logo.png");

    registerUIClient();

    bind(LeaderRedirect.class).in(Singleton.class);
    LifecycleModule.bindStartupAction(binder(), RedirectMonitor.class);
  }

  private void registerJQueryAssets() {
    registerAsset("bower_components/jquery/dist/jquery.js", "/js/jquery.min.js", false);
  }

  private void registerBootstrapAssets() {
    final String BOOTSTRAP_PATH = "bower_components/bootstrap/";

    registerAsset(BOOTSTRAP_PATH + "dist/js/bootstrap.min.js", "/js/bootstrap.min.js", false);
    registerAsset(BOOTSTRAP_PATH + "dist/css/bootstrap.min.css", "/css/bootstrap.min.css", false);

    registerAsset(BOOTSTRAP_PATH + "dist/fonts/glyphicons-halflings-regular.eot",
        "/fonts/glyphicons-halflings-regular.eot",
        false);
    registerAsset(BOOTSTRAP_PATH + "dist/fonts/glyphicons-halflings-regular.svg",
        "/fonts/glyphicons-halflings-regular.svg",
        false);
    registerAsset(BOOTSTRAP_PATH + "dist/fonts/glyphicons-halflings-regular.ttf",
        "/fonts/glyphicons-halflings-regular.ttf",
        false);
    registerAsset(BOOTSTRAP_PATH + "dist/fonts/glyphicons-halflings-regular.woff",
        "/fonts/glyphicons-halflings-regular.woff",
        false);
  }

  /**
   * A function to handle all assets related to the UI client.
   */
  private void registerUIClient() {
    registerAsset("bower_components/smart-table/Smart-Table.debug.js", "/js/smartTable.js", false);
    registerAsset("bower_components/angular/angular.js", "/js/angular.js", false);
    registerAsset("bower_components/angular-route/angular-route.js", "/js/angular-route.js", false);
    registerAsset("bower_components/underscore/underscore.js", "/js/underscore.js", false);
    registerAsset("bower_components/momentjs/moment.js", "/js/moment.js", false);
    registerAsset("bower_components/angular-bootstrap/ui-bootstrap-tpls.min.js",
        "/js/ui-bootstrap-tpls.js",
        false);

    registerAsset("ReadOnlyScheduler.js", "/js/readOnlyScheduler.js", false);
    registerAsset("api_types.js", "/js/apiTypes.js", false);
    registerAsset("thrift.js", "/js/thrift.js", false);

    registerAsset("ui/index.html", "/scheduler", true);
    Registration.registerEndpoint(binder(), "/scheduler");

    registerAsset("ui/roleLink.html", "/roleLink.html");
    registerAsset("ui/roleEnvLink.html", "/roleEnvLink.html");
    registerAsset("ui/jobLink.html", "/jobLink.html");
    registerAsset("ui/home.html", "/home.html");
    registerAsset("ui/role.html", "/role.html");
    registerAsset("ui/breadcrumb.html", "/breadcrumb.html");
    registerAsset("ui/error.html", "/error.html");
    registerAsset("ui/job.html", "/job.html");
    registerAsset("ui/taskSandbox.html", "/taskSandbox.html");
    registerAsset("ui/taskStatus.html", "/taskStatus.html");
    registerAsset("ui/taskLink.html", "/taskLink.html");
    registerAsset("ui/schedulingDetail.html", "/schedulingDetail.html");
    registerAsset("ui/groupSummary.html", "/groupSummary.html");
    registerAsset("ui/configSummary.html", "/configSummary.html");

    registerAsset("ui/css/app.css", "/css/app.css");

    registerAsset("ui/js/app.js", "/js/app.js");
    registerAsset("ui/js/controllers.js", "/js/controllers.js");
    registerAsset("ui/js/directives.js", "/js/directives.js");
    registerAsset("ui/js/services.js", "/js/services.js");
    registerAsset("ui/js/filters.js", "/js/filters.js");
  }

  private void registerAsset(String resourceLocation, String registerLocation) {
    registerAsset(resourceLocation, registerLocation, true);
  }

  private void registerAsset(String resourceLocation, String registerLocation, boolean isRelative) {
    String mediaType = getMediaType(resourceLocation).toString();

    if (isRelative) {
      Registration.registerHttpAsset(
          binder(),
          registerLocation,
          ServletModule.class,
          resourceLocation,
          mediaType,
          true);
    } else {
      Registration.registerHttpAsset(
          binder(),
          registerLocation,
          Resources.getResource(resourceLocation),
          mediaType,
          true);
    }
  }

  private MediaType getMediaType(String filePath) {
    if (filePath.endsWith(".png")) {
      return MediaType.PNG;
    } else if (filePath.endsWith(".js")) {
      return MediaType.JAVASCRIPT_UTF_8;
    } else if (filePath.endsWith(".html")) {
      return MediaType.HTML_UTF_8;
    } else if (filePath.endsWith(".css")) {
      return MediaType.CSS_UTF_8;
    } else if (filePath.endsWith(".svg")) {
      return MediaType.SVG_UTF_8;
    } else if (filePath.endsWith(".ttf")
        || filePath.endsWith(".eot")
        || filePath.endsWith(".woff")) {

      // MediaType doesn't have any mime types for fonts. Instead of magic strings, we let the
      // browser interpret the mime type and modern browsers can do this well.
      return MediaType.ANY_TYPE;
    } else {
      throw new IllegalArgumentException("Could not determine media type for " + filePath);
    }
  }

  static class RedirectMonitor implements ExceptionalCommand<MonitorException> {

    private final LeaderRedirect redirector;

    @Inject
    RedirectMonitor(LeaderRedirect redirector) {
      this.redirector = Preconditions.checkNotNull(redirector);
    }

    @Override
    public void execute() throws MonitorException {
      redirector.monitor();
    }
  }
}
