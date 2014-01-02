/**
 * Copyright 2013 Apache Software Foundation
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
package org.apache.aurora.scheduler.thrift.aop;

import java.lang.reflect.Method;

import javax.inject.Inject;

import com.google.common.base.Predicate;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.gen.ResponseCode;

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
