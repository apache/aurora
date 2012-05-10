package com.twitter.mesos;

import com.google.inject.Binder;
import com.google.inject.matcher.Matchers;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

/**
 * Utilities for guice configuration in mesos.
 */
public final class GuiceUtils {

  private GuiceUtils() {
    // utility
  }

  /**
   * Binds an interceptor that ensures the main ClassLoader is bound as the thread context
   * {@link ClassLoader} during JNI callbacks from the mesos core.  Some libraries require a thread
   * context ClassLoader be set and this ensures those libraries work properly.
   *
   * @param binder The binder to use to register an interceptor with.
   */
  public static void bindJNIContextClassLoader(Binder binder) {
    final ClassLoader mainClassLoader = GuiceUtils.class.getClassLoader();
    binder.bindInterceptor(Matchers.any(), Matchers.annotatedWith(JNICallback.class),
        new MethodInterceptor() {
          @Override public Object invoke(MethodInvocation invocation) throws Throwable {
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
}
