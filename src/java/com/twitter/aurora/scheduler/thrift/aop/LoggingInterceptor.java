package com.twitter.aurora.scheduler.thrift.aop;

import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;

import com.twitter.aurora.gen.JobConfiguration;
import com.twitter.aurora.gen.SessionKey;

/**
 * A method interceptor that logs all invocations as well as any unchecked exceptions thrown from
 * the underlying call.
 */
class LoggingInterceptor implements MethodInterceptor {

  private static final Logger LOG = Logger.getLogger(LoggingInterceptor.class.getName());

  // TODO(wfarner): Scrub updateToken when it is identifiable by type.
  private static final Map<Class<?>, Function<Object, String>> PRINT_FUNCTIONS =
      ImmutableMap.<Class<?>, Function<Object, String>>of(
          JobConfiguration.class,
          new Function<Object, String>() {
            @Override public String apply(Object input) {
              JobConfiguration configuration = ((JobConfiguration) input).deepCopy();
              if (configuration.isSetTaskConfig()) {
                configuration.getTaskConfig().setThermosConfig("BLANKED".getBytes());
              }
              return configuration.toString();
            }
          },
          SessionKey.class,
          new Function<Object, String>() {
            @Override public String apply(Object input) {
              SessionKey key = (SessionKey) input;
              return "SessionKey(user:" + key.getUser() + ")";
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
        Function<Object, String> printFunction = PRINT_FUNCTIONS.get(arg.getClass());
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
      throw e;
    }
  }
}
