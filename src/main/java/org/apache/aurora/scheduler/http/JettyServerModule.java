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

import java.util.EnumSet;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.annotation.Nonnegative;
import javax.inject.Inject;
import javax.inject.Singleton;
import javax.servlet.http.HttpServlet;
import javax.ws.rs.HttpMethod;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import com.google.inject.servlet.GuiceFilter;
import com.google.inject.servlet.GuiceServletContextListener;
import com.sun.jersey.api.container.filter.GZIPContentEncodingFilter;
import com.sun.jersey.guice.JerseyServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;
import com.twitter.common.application.modules.LifecycleModule;
import com.twitter.common.application.modules.LifecycleModule.LaunchException;
import com.twitter.common.args.Arg;
import com.twitter.common.args.CmdLine;
import com.twitter.common.base.Command;
import com.twitter.common.base.ExceptionalSupplier;
import com.twitter.common.base.MoreSuppliers;
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

import org.apache.aurora.scheduler.SchedulerServicesModule;
import org.apache.aurora.scheduler.http.api.ApiBeta;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.rewrite.handler.RewriteRegexRule;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.DispatcherType;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerCollection;
import org.eclipse.jetty.server.handler.RequestLogHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlets.GzipFilter;
import org.eclipse.jetty.util.resource.Resource;

import static java.util.Objects.requireNonNull;

import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_REQUEST_FILTERS;
import static com.sun.jersey.api.core.ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS;
import static com.sun.jersey.api.json.JSONConfiguration.FEATURE_POJO_MAPPING;
import static com.twitter.common.application.modules.LocalServiceRegistry.LocalService;

/**
 * Binding module for scheduler HTTP servlets.
 * <p>
 * TODO(wfarner): Continue improvements here by simplifying serving of static assets.  Jetty's
 * DefaultServlet can take over this responsibility, and jetty-rewite can be used to rewrite
 * requests (for static assets) similar to what we currently do with path specs.
 */
public class JettyServerModule extends AbstractModule {
  private static final Logger LOG = Logger.getLogger(JettyServerModule.class.getName());

  // The name of the request attribute where the path for the current request before it was
  // rewritten is stored.
  static final String ORIGINAL_PATH_ATTRIBUTE_NAME = "originalPath";

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

  private static final String ENTITIES_HELP_ROOT = Resource
      .newClassPathResource("org/apache/aurora/scheduler/storage/entities/help")
      .toString();
  private static final String API_CLIENT_ROOT = Resource
      .newClassPathResource("org/apache/aurora/scheduler/gen/client")
      .toString();
  private static final String STATIC_ASSETS_ROOT = Resource
      .newClassPathResource("scheduler/assets/index.html")
      .toString()
      .replace("assets/index.html", "");

  @Override
  protected void configure() {
    bind(Runnable.class)
        .annotatedWith(Names.named(AbortHandler.ABORT_HANDLER_KEY))
        .to(AbortCallback.class);
    bind(AbortCallback.class).in(Singleton.class);
    bind(Runnable.class).annotatedWith(Names.named(QuitHandler.QUIT_HANDLER_KEY))
        .to(QuitCallback.class);
    bind(QuitCallback.class).in(Singleton.class);
    bind(new TypeLiteral<ExceptionalSupplier<Boolean, ?>>() { })
        .annotatedWith(Names.named(HealthHandler.HEALTH_CHECKER_KEY))
        .toInstance(MoreSuppliers.ofInstance(true));

    bindConstant().annotatedWith(StringTemplateServlet.CacheTemplates.class).to(true);

    LifecycleModule.bindServiceRunner(binder(), HttpServerLauncher.class);

    bind(LeaderRedirect.class).in(Singleton.class);
    SchedulerServicesModule.addAppStartupServiceBinding(binder()).to(RedirectMonitor.class);
  }

  static class RedirectMonitor extends AbstractIdleService {
    private final LeaderRedirect redirector;

    @Inject
    RedirectMonitor(LeaderRedirect redirector) {
      this.redirector = requireNonNull(redirector);
    }

    @Override
    public void startUp() throws MonitorException {
      redirector.monitor();
    }

    @Override
    protected void shutDown() {
      // Nothing to do here - we await VM shutdown.
    }
  }

  // TODO(wfarner): Use guava's Service to enforce the lifecycle of this.
  public static final class HttpServerLauncher implements LifecycleModule.ServiceRunner {
    private final Injector parentInjector;

    @Inject
    HttpServerLauncher(Injector parentInjector) {
      this.parentInjector = requireNonNull(parentInjector);
    }

    private static final Map<String, String> REGEX_REWRITE_RULES =
        ImmutableMap.<String, String>builder()
            .put("/(?:index.html)?", "/assets/index.html")
            .put("/graphview(?:/index.html)?", "/assets/graphview/graphview.html")
            .put("/graphview/(.*)", "/assets/graphview/$1")
            .put("/(?:scheduler|updates)(?:/.*)?", "/assets/scheduler/index.html")
            .build();

    private RewriteHandler getRewriteHandler(HandlerCollection rootHandler) {
      RewriteHandler rewrites = new RewriteHandler();
      rewrites.setOriginalPathAttribute(ORIGINAL_PATH_ATTRIBUTE_NAME);
      rewrites.setRewriteRequestURI(true);
      rewrites.setRewritePathInfo(true);

      for (Map.Entry<String, String> entry : REGEX_REWRITE_RULES.entrySet()) {
        RewriteRegexRule rule = new RewriteRegexRule();
        rule.setRegex(entry.getKey());
        rule.setReplacement(entry.getValue());
        rewrites.addRule(rule);
      }

      rewrites.setHandler(rootHandler);

      return rewrites;
    }

    @Singleton
    static class ApiHelpResourceServlet extends DefaultServlet {
      // Subclass to allow extra instance of DefaultServlet.
    }

    @Singleton
    static class ApiClientServlet extends DefaultServlet {
      // Subclass to allow extra instance of DefaultServlet.
    }

    @Override
    public LocalService launch() throws LaunchException {
      // N.B. we explicitly disable the resource cache here due to a bug serving content out of the
      // jar under the vagrant image. C.f. https://bugs.eclipse.org/bugs/show_bug.cgi?id=364936
      Resource.setDefaultUseCaches(false);

      final Server server = new Server();
      ServletContextHandler servletHandler =
          new ServletContextHandler(server, "/", ServletContextHandler.NO_SESSIONS);

      servletHandler.addServlet(DefaultServlet.class, "/");
      servletHandler.addFilter(GuiceFilter.class,  "/*", EnumSet.allOf(DispatcherType.class));

      HandlerCollection rootHandler = new HandlerCollection();

      RequestLogHandler logHandler = new RequestLogHandler();
      logHandler.setRequestLog(new RequestLogger());

      rootHandler.addHandler(logHandler);
      rootHandler.addHandler(servletHandler);

      servletHandler.addEventListener(new GuiceServletContextListener() {
        @Override
        protected Injector getInjector() {
          return parentInjector.createChildInjector(new JerseyServletModule() {
            private void registerJerseyEndpoint(String indexPath, Class<?> servlet) {
              filter(indexPath + "*").through(LeaderRedirectFilter.class);
              filter(indexPath + "*").through(GuiceContainer.class, CONTAINER_PARAMS);
              bind(servlet);
            }

            private void registerServlet(String pathSpec, Class<? extends HttpServlet> servlet) {
              bind(servlet).in(Singleton.class);
              serve(pathSpec).with(servlet);
            }

            @Override
            protected void configureServlets() {
              bind(HttpStatsFilter.class).in(Singleton.class);
              filter("/assets/scheduler*").through(HttpStatsFilter.class);
              bind(LeaderRedirectFilter.class).in(Singleton.class);
              filterRegex("/assets/scheduler(?:/.*)?").through(LeaderRedirectFilter.class);

              // Add CORS support for all /api endpoints.
              if (ENABLE_CORS_SUPPORT.get()) {
                bind(CorsFilter.class).toInstance(new CorsFilter(ENABLE_CORS_FOR.get()));
                filter("/api*").through(CorsFilter.class);
              }

              // NOTE: GzipFilter is applied only to /api instead of globally because the
              // Jersey-managed servlets have a conflicting filter applied to them.
              bind(GzipFilter.class).in(Singleton.class);
              filterRegex("/api(?:/.*)?").through(GzipFilter.class, ImmutableMap.of(
                  "methods", Joiner.on(',').join(ImmutableSet.of(
                      HttpMethod.GET,
                      HttpMethod.POST))));
              filterRegex("/assets/.*").through(
                  GzipFilter.class,
                  ImmutableMap.of("methods", HttpMethod.GET));

              bind(DefaultServlet.class).in(Singleton.class);
              serve("/assets*")
                  .with(DefaultServlet.class, ImmutableMap.of(
                      "resourceBase", STATIC_ASSETS_ROOT,
                      "dirAllowed", "false"));

              serve("/apihelp/*")
                  .with(ApiHelpResourceServlet.class, ImmutableMap.<String, String>builder()
                      .put("resourceBase", ENTITIES_HELP_ROOT)
                      .put("pathInfoOnly", "true")
                      .put("dirAllowed", "false")
                      .build());

              serve("/apiclient/*")
                  .with(ApiClientServlet.class, ImmutableMap.<String, String>builder()
                      .put("resourceBase", API_CLIENT_ROOT)
                      .put("pathInfoOnly", "true")
                      .put("dirAllowed", "false")
                      .build());

              bind(GuiceContainer.class).in(Singleton.class);
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

              registerServlet("/abortabortabort", AbortHandler.class);
              registerServlet("/contention", ContentionPrinter.class);
              registerServlet("/health", HealthHandler.class);
              registerServlet("/logconfig", LogConfig.class);
              registerServlet("/quitquitquit", QuitHandler.class);
              registerServlet("/api", SchedulerAPIServlet.class);
              registerServlet("/threads", ThreadStackPrinter.class);
              registerServlet("/graphdata/", TimeSeriesDataSource.class);
              registerServlet("/vars", VarsHandler.class);
              registerServlet("/vars.json", VarsJsonHandler.class);
            }
          });
        }
      });

      Connector connector = new SelectChannelConnector();
      connector.setPort(HTTP_PORT.get());
      server.addConnector(connector);
      server.setHandler(getRewriteHandler(rootHandler));

      try {
        connector.open();
        server.start();
      } catch (Exception e) {
        throw new LaunchException("Failed to start jetty server: " + e, e);
      }

      Command shutdown = new Command() {
        @Override public void execute() {
          LOG.info("Shutting down embedded http server");
          try {
            server.stop();
          } catch (Exception e) {
            LOG.log(Level.INFO, "Failed to stop jetty server: " + e, e);
          }
        }
      };

      return LocalService.auxiliaryService("http", connector.getLocalPort(), shutdown);
    }
  }
}
