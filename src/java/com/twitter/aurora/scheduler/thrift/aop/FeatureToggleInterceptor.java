package com.twitter.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;

import com.google.common.base.Predicate;
import com.google.inject.Inject;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.aurora.gen.ResponseCode;

/**
 * A method interceptor that blocks access to features based on a supplied predicate.
 */
public class FeatureToggleInterceptor implements MethodInterceptor {

  @Inject private Predicate<Method> allowMethod;

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    Method method = invocation.getMethod();
    if (!allowMethod.apply(method)) {
      return Interceptors.properlyTypedResponse(
          method,
          ResponseCode.ERROR,
          "The " + method.getName() + " feature is currently disabled on this scheduler.");
    } else {
      return invocation.proceed();
    }
  }
}
