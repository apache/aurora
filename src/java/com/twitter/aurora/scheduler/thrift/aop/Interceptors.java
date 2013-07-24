package com.twitter.aurora.scheduler.thrift.aop;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;

import com.google.common.base.Throwables;

import com.twitter.aurora.gen.ResponseCode;

/**
 * Utility class for functions useful when implementing an interceptor on the thrift interface.
 */
final class Interceptors {

  private Interceptors() {
    // Utility class.
  }

  private static final Logger LOG = Logger.getLogger(Interceptors.class.getName());

  static Object properlyTypedResponse(Method method, ResponseCode responseCode, String message)
      throws IllegalAccessException, InstantiationException {

    Class<?> returnType = method.getReturnType();
    Object response = returnType.newInstance();
    invoke(returnType, response, "setResponseCode", ResponseCode.class, responseCode);
    invoke(returnType, response, "setMessage", String.class, message);
    return response;
  }

  private static <T> void invoke(
      Class<?> type,
      Object obj,
      String name,
      Class<T> parameterType,
      T argument) {

    Method method;
    try {
      method = type.getMethod(name, parameterType);
    } catch (NoSuchMethodException e) {
      LOG.severe(type + " does not support " + name);
      throw Throwables.propagate(e);
    }
    try {
      method.invoke(obj, argument);
    } catch (IllegalAccessException e) {
      LOG.severe("Method " + name + " is not accessible in " + type);
      throw Throwables.propagate(e);
    } catch (InvocationTargetException e) {
      LOG.severe("Failed to invoke " + name + " on " + type);
      throw Throwables.propagate(e);
    }
  }
}
