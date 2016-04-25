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
package org.apache.aurora.scheduler.resources;

import com.google.common.primitives.Longs;

/**
 * Converts resource values to/from generic (String) representation.
 * @param <T> Resource value type to convert.
 */
public interface ResourceTypeConverter<T> {
  /**
   * Parses resource value from the string representation.
   *
   * @param value String value to parse.
   * @return Resource value of type {@code T}.
   */
  T parseFrom(String value);

  /**
   * Converts resource of type {@code T} to its string representation.
   *
   * @param value Resource value to stringify.
   * @return String representation of the resource value.
   */
  default String stringify(Object value) {
    return value.toString();
  }

  LongConverter LONG = new LongConverter();
  DoubleConverter DOUBLE = new DoubleConverter();
  StringConverter STRING = new StringConverter();

  class LongConverter implements ResourceTypeConverter<Long> {
    @Override
    public Long parseFrom(String value) {
      return Longs.tryParse(value);
    }
  }

  class DoubleConverter implements ResourceTypeConverter<Double> {
    @Override
    public Double parseFrom(String value) {
      return Double.parseDouble(value);
    }
  }

  class StringConverter implements ResourceTypeConverter<String> {
    @Override
    public String parseFrom(String value) {
      return value;
    }
  }
}
