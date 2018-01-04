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
package org.apache.aurora.scheduler.storage.durability;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;

import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;
import org.apache.thrift.TUnion;

final class Generator {
  private Generator() {
    // utility class.
  }

  private static Map<Class<?>, Object> simple = ImmutableMap.<Class<?>, Object>builder()
      .put(boolean.class, true)
      .put(Boolean.class, true)
      .put(int.class, 2)
      .put(Integer.class, 2)
      .put(long.class, 4L)
      .put(Long.class, 4L)
      .put(double.class, 8.0)
      .put(Double.class, 8.0)
      .put(String.class, "string-value")
      .build();

  /**
   * Generates a value of the given type.
   *
   * @param clazz The non-generic type to generate.
   * @param parameterizedType The possibly-generic type (with type parameter information).
   * @return A synthetically-generated and fully-populated instance.
   * @throws ReflectiveOperationException If generation failed due to a reflection issue.
   */
  static Object valueFor(Class<?> clazz, Type parameterizedType)
      throws ReflectiveOperationException {

    if (simple.containsKey(clazz)) {
      return simple.get(clazz);
    }

    if (TBase.class.isAssignableFrom(clazz)) {
      @SuppressWarnings("unchecked")
      Class<TBase<?, ?>> tbase = (Class<TBase<?, ?>>) parameterizedType;
      return newStruct(tbase);
    } else if (TEnum.class.isAssignableFrom(clazz)) {
      Method values = clazz.getDeclaredMethod("values");
      Object[] result = (Object[]) values.invoke(null);
      return result[0];
    } else if (Collection.class.isAssignableFrom(clazz)) {
      ParameterizedType parameterized = (ParameterizedType) parameterizedType;
      Type[] arguments = parameterized.getActualTypeArguments();
      if (clazz == List.class) {
        return ImmutableList.of(valueFor((Class) arguments[0], arguments[0]));
      } else if (clazz == Set.class) {
        return ImmutableSet.of(valueFor((Class) arguments[0], arguments[0]));
      } else {
        throw new IllegalArgumentException();
      }
    } else if (Map.class.isAssignableFrom(clazz)) {
      ParameterizedType parameterized = (ParameterizedType) parameterizedType;
      Type[] arguments = parameterized.getActualTypeArguments();
      return ImmutableMap.of(
          valueFor((Class) arguments[0], arguments[0]),
          valueFor((Class) arguments[1], arguments[1]));
    } else {
      throw new IllegalArgumentException("Unsupported type " + parameterizedType);
    }
  }

  private static <T extends TBase<?, ?>> void setValue(T struct, Method setter) {
    try {
      // All setters are expected to have exactly one parameter.
      Class<?> paramType = setter.getParameterTypes()[0];
      Type genericParamType = setter.getGenericParameterTypes()[0];

      setter.invoke(struct, valueFor(paramType, genericParamType));
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Generates a thrift struct of the given tuype.
   *
   * @param structClass Struct type.
   * @param <T> Struct type.
   * @return A populated instance.
   */
  static <T extends TBase<?, ?>> T newStruct(Class<T> structClass) {
    T struct;
    try {
      struct = structClass.newInstance();
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }

    Stream<Method> setterMethods = Stream.of(structClass.getDeclaredMethods())
        // Order methods by name for predictable behavior.  This is particularly useful for
        // deterministic behavior of picking a TUnion field.
        .sorted(Ordering.natural().onResultOf(Method::getName))
        .filter(method -> !method.getName().equals("setFieldValue"))
        .filter(method -> method.getName().startsWith("set"))
        .filter(method -> !method.getName().endsWith("IsSet"));

    if (TUnion.class.isAssignableFrom(structClass)) {
      // A TUnion can represent one of many types.  Choose one arbitrarily.
      setterMethods = setterMethods
          .limit(1);
    }

    setterMethods.forEach(setter -> {
      setValue(struct, setter);
    });

    return struct;
  }
}
