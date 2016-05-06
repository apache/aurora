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
 * Converts Aurora resource values to/from generic (String) representation.
 * @param <T> Resource value type to convert.
 */
public interface AuroraResourceConverter<T> {
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

  /**
   * Gets resource quantity.
   *
   * @param value Value to quantify.
   * @return Resource quantity.
   */
  Double quantify(Object value);

  /**
   * Converts resource quantity to matching resource value type (if such conversion exists).
   *
   * @param value Resource quantity.
   * @return Value of type T.
   */
  T valueOf(Double value);

  LongConverter LONG = new LongConverter();
  DoubleConverter DOUBLE = new DoubleConverter();
  StringConverter STRING = new StringConverter();

  class LongConverter implements AuroraResourceConverter<Long> {
    @Override
    public Long parseFrom(String value) {
      return Longs.tryParse(value);
    }

    @Override
    public Double quantify(Object value) {
      return (double) (long) value;
    }

    @Override
    public Long valueOf(Double value) {
      return value.longValue();
    }
  }

  class DoubleConverter implements AuroraResourceConverter<Double> {
    @Override
    public Double parseFrom(String value) {
      return Double.parseDouble(value);
    }

    @Override
    public Double quantify(Object value) {
      return (Double) value;
    }

    @Override
    public Double valueOf(Double value) {
      return value;
    }
  }

  class StringConverter implements AuroraResourceConverter<String> {
    @Override
    public String parseFrom(String value) {
      return value;
    }

    @Override
    public Double quantify(Object value) {
      return 1.0;
    }

    @Override
    public String valueOf(Double value) {
      throw new UnsupportedOperationException("Unsupported for string resource types");
    }
  }
}
