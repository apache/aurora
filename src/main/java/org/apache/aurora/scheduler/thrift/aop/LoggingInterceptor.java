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

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.aurora.gen.ExecutorConfig;
import org.apache.aurora.gen.JobConfiguration;
import org.apache.aurora.gen.JobUpdateRequest;
import org.apache.aurora.gen.ResponseCode;
import org.apache.aurora.scheduler.storage.Storage;
import org.apache.aurora.scheduler.thrift.Responses;
import org.apache.shiro.ShiroException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A method interceptor that logs all invocations as well as any unchecked exceptions thrown from
 * the underlying call.
 */
class LoggingInterceptor implements MethodInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingInterceptor.class);

  private final Map<Class<?>, Function<Object, String>> printFunctions =
      ImmutableMap.of(
          JobConfiguration.class,
          input -> {
            JobConfiguration configuration = ((JobConfiguration) input).deepCopy();
            if (configuration.isSetTaskConfig()) {
              configuration.getTaskConfig().setExecutorConfig(
                  new ExecutorConfig("BLANKED", "BLANKED"));
            }
            return configuration.toString();
          },
          JobUpdateRequest.class,
          input -> {
            JobUpdateRequest configuration = ((JobUpdateRequest) input).deepCopy();
            if (configuration.isSetTaskConfig()) {
              configuration.getTaskConfig().setExecutorConfig(
                  new ExecutorConfig("BLANKED", "BLANKED"));
            }
            return configuration.toString();
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
    String messageArgs = String.join(", ", argStrings);
    LOG.info("{}({})", methodName, messageArgs);
    try {
      return invocation.proceed();
    } catch (Storage.TransientStorageException e) {
      LOG.warn("Uncaught transient exception while handling {}({})", methodName, messageArgs, e);
      return Responses.addMessage(Responses.empty(), ResponseCode.ERROR_TRANSIENT, e);
    } catch (RuntimeException e) {
      // We need shiro's exceptions to bubble up to the Shiro servlet filter so we intentionally
      // do not swallow them here.
      Throwables.throwIfInstanceOf(e, ShiroException.class);
      LOG.warn("Uncaught exception while handling {}({})", methodName, messageArgs, e);
      return Responses.addMessage(Responses.empty(), ResponseCode.ERROR, e);
    }
  }
}
