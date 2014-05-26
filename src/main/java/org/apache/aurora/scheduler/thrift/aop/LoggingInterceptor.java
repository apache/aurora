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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.auth.CapabilityValidator;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.gen.SessionKey;

import static org.apache.aurora.scheduler.thrift.aop.Interceptors.properlyTypedResponse;

/**
 * A method interceptor that logs all invocations as well as any unchecked exceptions thrown from
 * the underlying call.
 */
class LoggingInterceptor implements MethodInterceptor {

  private static final Logger LOG = Logger.getLogger(LoggingInterceptor.class.getName());

  @Inject private CapabilityValidator validator;

  // TODO(wfarner): Scrub updateToken when it is identifiable by type.
  private final Map<Class<?>, Function<Object, String>> printFunctions =
      ImmutableMap.<Class<?>, Function<Object, String>>of(
          JobConfiguration.class,
          new Function<Object, String>() {
            @Override
            public String apply(Object input) {
              JobConfiguration configuration = ((JobConfiguration) input).deepCopy();
              if (configuration.isSetTaskConfig()) {
                configuration.getTaskConfig().setExecutorConfig(
                    new ExecutorConfig("BLANKED", "BLANKED"));
              }
              return configuration.toString();
            }
          },
          SessionKey.class,
          new Function<Object, String>() {
            @Override
            public String apply(Object input) {
              SessionKey key = (SessionKey) input;
              return validator.toString(key);
            }
          }
      );

  @Override
  public Object invoke(MethodInvocation invocation) throws Throwable {
    List<String> argStrings = Lists.newArrayList();
    for (Object arg : invocation.getArguments()) {
      if (arg == null) {
        argStrings.add("null");
      } else {
        Function<Object, String> printFunction = printFunctions.get(arg.getClass());
        argStrings.add((printFunction == null) ? arg.toString() : printFunction.apply(arg));
      }
    }
    String methodName = invocation.getMethod().getName();
    String message = String.format("%s(%s)", methodName, Joiner.on(", ").join(argStrings));
    LOG.info(message);
    try {
      return invocation.proceed();
    } catch (RuntimeException e) {
      LOG.log(Level.WARNING, "Uncaught exception while handling " + message, e);
      return properlyTypedResponse(invocation.getMethod(), ResponseCode.ERROR, e.getMessage());
    }
  }
}
