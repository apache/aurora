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
package org.apache.aurora.scheduler.thrift.aop;

import java.util.List;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.aurora.gen.Response;
import org.apache.aurora.scheduler.app.MoreModules;
import org.apache.aurora.scheduler.config.CliOptions;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;

/**
 * Binding module for AOP-style decorations of the thrift API.
 */
public class AopModule extends AbstractModule {

  @Parameters(separators = "=")
  public static class Options {
    @Parameter(names = "-thrift_method_interceptor_modules",
        description = "Custom Guice module(s) to provide additional Thrift method interceptors.")
    @SuppressWarnings("rawtypes")
    public List<Class> methodInterceptorModules = ImmutableList.of();
  }

  public static final Matcher<? super Class<?>> THRIFT_IFACE_MATCHER =
      Matchers.subclassesOf(AnnotatedAuroraAdmin.class)
          .and(Matchers.annotatedWith(DecoratedThrift.class));

  private final CliOptions options;

  public AopModule(CliOptions options) {
    this.options = options;
  }

  @Override
  protected void configure() {
    // Layer ordering:
    // APIVersion -> Log -> StatsExporter -> custom interceptors -> SchedulerThriftInterface

    // It's important for this interceptor to be registered first to ensure it's at the 'top' of
    // the stack and the standard message is always applied.
    bindThriftDecorator(new ServerInfoInterceptor());

    bindThriftDecorator(new LoggingInterceptor());
    bindThriftDecorator(new ThriftStatsExporterInterceptor());

    // Install custom interceptor modules
    for (Module module
        : MoreModules.instantiateAll(options.aop.methodInterceptorModules, options)) {

      install(module);
    }
  }

  private void bindThriftDecorator(MethodInterceptor interceptor) {
    bindThriftDecorator(binder(), THRIFT_IFACE_MATCHER, interceptor);
  }

  public static void bindThriftDecorator(
      Binder binder,
      Matcher<? super Class<?>> classMatcher,
      MethodInterceptor interceptor) {

    binder.bindInterceptor(
        classMatcher,
        Matchers.returns(Matchers.subclassesOf(Response.class)),
        interceptor);
    binder.requestInjection(interceptor);
  }
}
