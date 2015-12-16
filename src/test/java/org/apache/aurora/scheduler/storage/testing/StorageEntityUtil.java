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
package org.apache.aurora.scheduler.storage.testing;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Defaults;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.gson.internal.Primitives;

import org.apache.thrift.TUnion;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Utility for validating objects used in storage testing.
 */
public final class StorageEntityUtil {

  private StorageEntityUtil() {
    // Utility class.
  }

  private static void assertFullyPopulated(String name, Object object, Set<Field> ignoredFields) {
    if (object instanceof Collection) {
      Object[] values = ((Collection<?>) object).toArray();
      assertFalse("Collection is empty: " + name, values.length == 0);
      for (int i = 0; i < values.length; i++) {
        assertFullyPopulated(name + "[" + i + "]", values[i], ignoredFields);
      }
    } else if (object instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) object;
      assertFalse("Map is empty: " + name, map.isEmpty());
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        assertFullyPopulated(name + " key", entry.getKey(), ignoredFields);
        assertFullyPopulated(name + "[" + entry.getKey() + "]", entry.getValue(), ignoredFields);
      }
    } else if (object instanceof TUnion) {
      TUnion<?, ?> union = (TUnion<?, ?>) object;
      assertFullyPopulated(
          name + "." + union.getSetField().getFieldName(),
          union.getFieldValue(),
          ignoredFields);
    } else if (!(object instanceof String) && !(object instanceof Enum)) {
      for (Field field : object.getClass().getDeclaredFields()) {
        if (!Modifier.isStatic(field.getModifiers())) {
          validateField(name, object, field, ignoredFields);
        }
      }
    }
  }

  private static void validateField(
      String name,
      Object object,
      Field field,
      Set<Field> ignoredFields) {

    try {
      field.setAccessible(true);
      String fullName = name + "." + field.getName();
      Object fieldValue = field.get(object);
      boolean mustBeSet = !ignoredFields.contains(field);
      if (mustBeSet) {
        assertNotNull(fullName + " is null", fieldValue);
      }
      if (fieldValue != null) {
        if (Primitives.isWrapperType(fieldValue.getClass())) {
          // Special-case the mutable hash code field.
          if (mustBeSet && !fullName.endsWith("cachedHashCode")) {
            assertNotEquals(
                "Primitive value must not be default: " + fullName,
                Defaults.defaultValue(Primitives.unwrap(fieldValue.getClass())),
                fieldValue);
          }
        } else {
          assertFullyPopulated(fullName, fieldValue, ignoredFields);
        }
      }
    } catch (IllegalAccessException e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Ensures that an object tree is fully-populated.  This is useful when testing store
   * implementations to validate that all fields are mapped during a round-trip into and out of
   * a store implementation.
   *
   * @param object Object to ensure is fully populated.
   * @param <T> Object type.
   * @return The original {@code object}.
   */
  public static <T> T assertFullyPopulated(T object, Field... ignoredFields) {
    assertFullyPopulated(
        object.getClass().getSimpleName(),
        object,
        ImmutableSet.copyOf(ignoredFields));
    return object;
  }

  /**
   * Convenience method to get a field by name from a class, to pass as an ignored field to
   * {@link #assertFullyPopulated(Object, Field...)}.
   *
   * @param clazz Class to get a field from.
   * @param field Field name.
   * @return Field with the given {@code name}.
   */
  public static Field getField(Class<?> clazz, String field) {
    for (Field f : clazz.getDeclaredFields()) {
      if (f.getName().equals(field)) {
        return f;
      }
    }
    throw new IllegalArgumentException("Field not found: " + field);
  }
}
