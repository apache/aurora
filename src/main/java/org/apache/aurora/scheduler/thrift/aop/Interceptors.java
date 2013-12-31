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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.logging.Logger;

import com.google.common.base.Throwables;

import org.apache.aurora.gen.ResponseCode;

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
