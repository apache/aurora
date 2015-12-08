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

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Singleton;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.AbstractModule;
import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.PrivateModule;
import com.google.inject.TypeLiteral;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;
import org.apache.aurora.common.args.Arg;
import org.apache.aurora.common.args.CmdLine;
import org.apache.aurora.gen.AuroraSchedulerManager;
import org.apache.aurora.gen.Response;
import org.apache.aurora.scheduler.thrift.auth.DecoratedThrift;

/**
 * Binding module for AOP-style decorations of the thrift API.
 */
public class AopModule extends AbstractModule {

  @CmdLine(name = "enable_job_updates", help = "Whether new job updates should be accepted.")
  private static final Arg<Boolean> ENABLE_UPDATES = Arg.create(true);

  @CmdLine(name = "enable_job_creation",
      help = "Allow new jobs to be created, if false all job creation requests will be denied.")
  private static final Arg<Boolean> ENABLE_JOB_CREATION = Arg.create(true);

  private static final Matcher<? super Class<?>> THRIFT_IFACE_MATCHER =
      Matchers.subclassesOf(AnnotatedAuroraAdmin.class)
          .and(Matchers.annotatedWith(DecoratedThrift.class));

  private final Map<String, Boolean> toggledMethods;

  public AopModule() {
    this(ImmutableMap.of(
        "createJob", ENABLE_JOB_CREATION.get(),
        "acquireLock", ENABLE_UPDATES.get()));
  }

  @VisibleForTesting
  AopModule(Map<String, Boolean> toggledMethods) {
    this.toggledMethods = ImmutableMap.copyOf(toggledMethods);
  }

  private static final Function<Method, String> GET_NAME = Method::getName;

  @Override
  protected void configure() {
    // Layer ordering:
    // APIVersion -> Log -> FeatureToggle -> StatsExporter -> SchedulerThriftInterface

    // It's important for this interceptor to be registered first to ensure it's at the 'top' of
    // the stack and the standard message is always applied.
    bindThriftDecorator(new ServerInfoInterceptor());

    bindThriftDecorator(new LoggingInterceptor());

    install(new PrivateModule() {
      @Override
      protected void configure() {
        // Ensure that the provided methods exist on the decorated interface.
        List<Method> methods =
            ImmutableList.copyOf(AuroraSchedulerManager.Iface.class.getMethods());
        for (String toggledMethod : toggledMethods.keySet()) {
          Preconditions.checkArgument(
              Iterables.any(methods,
                  Predicates.compose(Predicates.equalTo(toggledMethod), GET_NAME)),
              String.format("Method %s was not found in class %s",
                  toggledMethod,
                  AuroraSchedulerManager.Iface.class));
        }

        bind(new TypeLiteral<Map<String, Boolean>>() { }).toInstance(toggledMethods);
        bind(IsFeatureEnabled.class).in(Singleton.class);
        Key<Predicate<Method>> predicateKey = Key.get(new TypeLiteral<Predicate<Method>>() { });
        bind(predicateKey).to(IsFeatureEnabled.class);
        expose(predicateKey);
      }
    });
    bindThriftDecorator(new FeatureToggleInterceptor());
    bindThriftDecorator(new ThriftStatsExporterInterceptor());
  }

  private void bindThriftDecorator(MethodInterceptor interceptor) {
    bindThriftDecorator(binder(), THRIFT_IFACE_MATCHER, interceptor);
  }

  @VisibleForTesting
  static void bindThriftDecorator(
      Binder binder,
      Matcher<? super Class<?>> classMatcher,
      MethodInterceptor interceptor) {

    binder.bindInterceptor(
        classMatcher,
        Matchers.returns(Matchers.subclassesOf(Response.class)),
        interceptor);
    binder.requestInjection(interceptor);
  }

  private static class IsFeatureEnabled implements Predicate<Method> {
    private final Predicate<String> methodEnabled;

    @Inject
    IsFeatureEnabled(Map<String, Boolean> toggleMethods) {
      Predicate<String> builder = Predicates.alwaysTrue();
      for (Map.Entry<String, Boolean> toggleMethod : toggleMethods.entrySet()) {
        Predicate<String> enableMethod = Predicates.or(
            toggleMethod.getValue()
                ? Predicates.alwaysTrue()
                : Predicates.alwaysFalse(),
            Predicates.not(Predicates.equalTo(toggleMethod.getKey())));
        builder = Predicates.and(builder, enableMethod);
      }
      methodEnabled = builder;
    }

    @Override
    public boolean apply(Method method) {
      return methodEnabled.apply(method.getName());
    }
  }
}
