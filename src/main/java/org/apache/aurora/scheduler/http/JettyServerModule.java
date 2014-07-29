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
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServlet;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import com.google.common.net.MediaType;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.twitter.common.application.http.DefaultQuitHandler;
import com.twitter.common.application.http.GraphViewer;
import com.twitter.common.application.http.HttpAssetConfig;
import com.twitter.common.application.http.HttpFilterConfig;
import com.twitter.common.application.http.HttpServletConfig;
import com.twitter.common.application.http.Registration;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalCommand;
import com.twitter.common.base.ExceptionalSupplier;
import com.twitter.common.base.MoreSuppliers;
import com.twitter.common.net.http.HttpServerDispatch;
import com.twitter.common.net.http.RequestLogger;
import com.twitter.common.net.http.handlers.AbortHandler;
import com.twitter.common.net.http.handlers.ContentionPrinter;
import com.twitter.common.net.http.handlers.HealthHandler;
import com.twitter.common.net.http.handlers.LogConfig;
import com.twitter.common.net.http.handlers.QuitHandler;
import com.twitter.common.net.http.handlers.StringTemplateServlet;
import com.twitter.common.net.http.handlers.ThreadStackPrinter;
import com.twitter.common.net.http.handlers.TimeSeriesDataSource;
import com.twitter.common.net.http.handlers.VarsHandler;
import com.twitter.common.net.http.handlers.VarsJsonHandler;
import com.twitter.common.net.pool.DynamicHostSet.MonitorException;

import org.apache.aurora.scheduler.http.api.ApiBeta;
import org.mortbay.jetty.RequestLog;
import org.mortbay.servlet.GzipFilter;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS;
import static com.sun.jersey.api.json.JSONConfiguration.FEATURE_POJO_MAPPING;
import static com.twitter.common.application.modules.LocalServiceRegistry.LocalService;

/**
 * Binding module for scheduler HTTP servlets.
 * <p>
 * TODO(wfarner): Continue improvements here by simplifying serving of static assets.  Jetty's
 * DefaultServlet can take over this responsibility.
 */
public class JettyServerModule extends AbstractModule {

  private static final Logger LOG = Logger.getLogger(JettyServerModule.class.getName());

  @Nonnegative
  @CmdLine(name = "http_port",
      help = "The port to start an HTTP server on.  Default value will choose a random port.")
  protected static final Arg<Integer> HTTP_PORT = Arg.create(0);

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
    bind(Runnable.class)
        .annotatedWith(Names.named(AbortHandler.ABORT_HANDLER_KEY))
        .to(AbortCallback.class);
    bind(AbortCallback.class).in(Singleton.class);
    bind(Runnable.class).annotatedWith(Names.named(QuitHandler.QUIT_HANDLER_KEY))
        .to(DefaultQuitHandler.class);
    bind(DefaultQuitHandler.class).in(Singleton.class);
    bind(new TypeLiteral<ExceptionalSupplier<Boolean, ?>>() { })
        .annotatedWith(Names.named(HealthHandler.HEALTH_CHECKER_KEY))
        .toInstance(MoreSuppliers.ofInstance(true));

    bindConstant().annotatedWith(StringTemplateServlet.CacheTemplates.class).to(true);

    bind(HttpServerDispatch.class).in(Singleton.class);
    bind(RequestLog.class).to(RequestLogger.class);
    Registration.registerServlet(binder(), "/abortabortabort", AbortHandler.class, true);
    Registration.registerServlet(binder(), "/contention", ContentionPrinter.class, false);
    Registration.registerServlet(binder(), "/graphdata", TimeSeriesDataSource.class, true);
    Registration.registerServlet(binder(), "/health", HealthHandler.class, true);
    Registration.registerServlet(binder(), "/healthz", HealthHandler.class, true);
    Registration.registerServlet(binder(), "/logconfig", LogConfig.class, false);
    Registration.registerServlet(binder(), "/quitquitquit", QuitHandler.class, true);
    Registration.registerServlet(binder(), "/threads", ThreadStackPrinter.class, false);
    Registration.registerServlet(binder(), "/vars", VarsHandler.class, false);
    Registration.registerServlet(binder(), "/vars.json", VarsJsonHandler.class, false);

    GraphViewer.registerResources(binder());

    LifecycleModule.bindServiceRunner(binder(), HttpServerLauncher.class);

    // Ensure at least an empty filter set is bound.
    Registration.getFilterBinder(binder());

    // Ensure at least an empty set of additional links is bound.
    Registration.getEndpointBinder(binder());

    // Register /api end point
    Registration.registerServlet(binder(), "/api", SchedulerAPIServlet.class, true);

    // NOTE: GzipFilter is applied only to /api instead of globally because the Jersey-managed
    // servlets have a conflicting filter applied to them.
    Registration.registerServletFilter(binder(), GzipFilter.class, "/api/*");
    Registration.registerServletFilter(binder(), GzipFilter.class, "/apibetabeta/*");
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
        filterRegex("/scheduler(?:/.*)?").through(LeaderRedirectFilter.class);

        // Add CORS support for all /api end points.
        if (ENABLE_CORS_SUPPORT.get()) {
          bind(CorsFilter.class).toInstance(new CorsFilter(ENABLE_CORS_FOR.get()));
          filter("/api*").through(CorsFilter.class);
        }

        registerJerseyEndpoint("/apibeta", ApiBeta.class);
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
          JettyServerModule.class,
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
      this.redirector = Objects.requireNonNull(redirector);
    }

    @Override
    public void execute() throws MonitorException {
      redirector.monitor();
    }
  }

  // TODO(wfarner): Use guava's Service to enforce the lifecycle of this.
  public static final class HttpServerLauncher implements LifecycleModule.ServiceRunner {
    private final HttpServerDispatch httpServer;
    private final Set<HttpServletConfig> httpServlets;
    private final Set<HttpAssetConfig> httpAssets;
    private final Set<HttpFilterConfig> httpFilters;
    private final Set<String> additionalIndexLinks;
    private final Injector injector;

    @Inject
    HttpServerLauncher(
        HttpServerDispatch httpServer,
        Set<HttpServletConfig> httpServlets,
        Set<HttpAssetConfig> httpAssets,
        Set<HttpFilterConfig> httpFilters,
        @Registration.IndexLink Set<String> additionalIndexLinks,
        Injector injector) {

      this.httpServer = checkNotNull(httpServer);
      this.httpServlets = checkNotNull(httpServlets);
      this.httpAssets = checkNotNull(httpAssets);
      this.httpFilters = checkNotNull(httpFilters);
      this.additionalIndexLinks = checkNotNull(additionalIndexLinks);
      this.injector = checkNotNull(injector);
    }

    @Override
    public LocalService launch() {
      if (!httpServer.listen(HTTP_PORT.get())) {
        throw new IllegalStateException("Failed to start HTTP server, all servlets disabled.");
      }

      for (HttpServletConfig config : httpServlets) {
        HttpServlet handler = injector.getInstance(config.handlerClass);
        httpServer.registerHandler(config.path, handler, config.params, config.silent);
      }

      for (HttpAssetConfig config : httpAssets) {
        httpServer.registerHandler(config.path, config.handler, null, config.silent);
      }

      for (HttpFilterConfig filter : httpFilters) {
        httpServer.registerFilter(filter.filterClass, filter.pathSpec, filter.dispatch);
      }

      for (String indexLink : additionalIndexLinks) {
        httpServer.registerIndexLink(indexLink);
      }

      Command shutdown = new Command() {
        @Override public void execute() {
          LOG.info("Shutting down embedded http server");
          httpServer.stop();
        }
      };

      return LocalService.auxiliaryService("http", httpServer.getPort(), shutdown);
    }
  }
}
