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
package org.apache.aurora;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.util.Set;

import javax.inject.Qualifier;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.matcher.AbstractMatcher;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.common.collections.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Utilities for guice configuration in aurora.
 */
public final class GuiceUtils {

  private static final Logger LOG = LoggerFactory.getLogger(GuiceUtils.class);

  // Method annotation that allows a trapped interface to whitelist methods that may throw
  // unchecked exceptions.
  @Qualifier
  @Target(METHOD) @Retention(RUNTIME)
  public @interface AllowUnchecked { }

  private GuiceUtils() {
    // utility
  }

  private static final Function<Method, Pair<String, Class<?>[]>> CANONICALIZE =
      method -> Pair.of(method.getName(), method.getParameterTypes());

  /**
   * Creates a matcher that will match methods of an interface, optionally excluding inherited
   * methods.
   *
   * @param matchInterface The interface to match.
   * @param declaredMethodsOnly if {@code true} only methods directly declared in the interface
   *                            will be matched, otherwise all methods on the interface are matched.
   * @return A new matcher instance.
   */
  public static Matcher<Method> interfaceMatcher(
      Class<?> matchInterface,
      boolean declaredMethodsOnly) {

    Method[] methods =
        declaredMethodsOnly ? matchInterface.getDeclaredMethods() : matchInterface.getMethods();
    final Set<Pair<String, Class<?>[]>> interfaceMethods =
        ImmutableSet.copyOf(Iterables.transform(ImmutableList.copyOf(methods), CANONICALIZE));
    final LoadingCache<Method, Pair<String, Class<?>[]>> cache = CacheBuilder.newBuilder()
        .build(CacheLoader.from(CANONICALIZE));

    return new AbstractMatcher<Method>() {
      @Override
      public boolean matches(Method method) {
        return interfaceMethods.contains(cache.getUnchecked(method));
      }
    };
  }

  /**
   * Binds an interceptor that ensures the main ClassLoader is bound as the thread context
   * {@link ClassLoader} during JNI callbacks from mesos.  Some libraries require a thread
   * context ClassLoader be set and this ensures those libraries work properly.
   *
   * @param binder The binder to use to register an interceptor with.
   * @param wrapInterface Interface whose methods should wrapped.
   */
  public static void bindJNIContextClassLoader(Binder binder, Class<?> wrapInterface) {
    final ClassLoader mainClassLoader = GuiceUtils.class.getClassLoader();
    binder.bindInterceptor(
        Matchers.subclassesOf(wrapInterface),
        interfaceMatcher(wrapInterface, false),
        new MethodInterceptor() {
          @Override
          public Object invoke(MethodInvocation invocation) throws Throwable {
            Thread currentThread = Thread.currentThread();
            ClassLoader prior = currentThread.getContextClassLoader();
            try {
              currentThread.setContextClassLoader(mainClassLoader);
              return invocation.proceed();
            } finally {
              currentThread.setContextClassLoader(prior);
            }
          }
        });
  }

  private static final Predicate<Method> IS_WHITELISTED =
      method -> method.getAnnotation(AllowUnchecked.class) != null;

  private static final Matcher<Method> WHITELIST_MATCHER = new AbstractMatcher<Method>() {
    @Override
    public boolean matches(Method method) {
      return IS_WHITELISTED.apply(method);
    }
  };

  private static final Predicate<Method> VOID_METHOD =
      method -> method.getReturnType() == Void.TYPE;

  /**
   * Binds an exception trap on all interface methods of all classes bound against an interface.
   * Individual methods may opt out of trapping by annotating with {@link AllowUnchecked}.
   * Only void methods are allowed, any non-void interface methods must explicitly opt out.
   *
   * @param binder The binder to register an interceptor with.
   * @param wrapInterface Interface whose methods should be wrapped.
   * @throws IllegalArgumentException If any of the non-whitelisted interface methods are non-void.
   */
  public static void bindExceptionTrap(Binder binder, Class<?> wrapInterface)
      throws IllegalArgumentException {

    Set<Method> disallowed = ImmutableSet.copyOf(Iterables.filter(
        ImmutableList.copyOf(wrapInterface.getMethods()),
        Predicates.and(Predicates.not(IS_WHITELISTED), Predicates.not(VOID_METHOD))));
    Preconditions.checkArgument(disallowed.isEmpty(),
        "Non-void methods must be explicitly whitelisted with @AllowUnchecked: %s", disallowed);

    Matcher<Method> matcher =
        Matchers.not(WHITELIST_MATCHER).and(interfaceMatcher(wrapInterface, false));
    binder.bindInterceptor(Matchers.subclassesOf(wrapInterface), matcher,
        new MethodInterceptor() {
          @Override
          public Object invoke(MethodInvocation invocation) throws Throwable {
            try {
              return invocation.proceed();
            } catch (RuntimeException e) {
              LOG.warn("Trapped uncaught exception: " + e, e);
              return null;
            }
          }
        });
  }
}
