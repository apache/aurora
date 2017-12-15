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
package org.apache.aurora.scheduler.http.api.security;

import java.util.Optional;

import com.google.common.base.Function;

import static java.util.Objects.requireNonNull;

/**
 * Function for retrieving an optional field from a thrift struct with runtime type-information.
 *
 * @param <T> the container struct
 * @param <V> a field that can be contained within T
 */
interface FieldGetter<T, V> extends Function<T, Optional<V>> {
  /**
   * The type of the container struct.
   */
  Class<T> getStructClass();

  /**
   * The type of the optionally-contained struct.
   */
  Class<V> getValueClass();

  abstract class AbstractFieldGetter<T, V> implements FieldGetter<T, V> {
    private final Class<T> structClass;
    private final Class<V> valueClass;

    protected AbstractFieldGetter(Class<T> structClass, Class<V> valueClass) {
      this.structClass = requireNonNull(structClass);
      this.valueClass = requireNonNull(valueClass);
    }

    @Override
    public final Class<T> getStructClass() {
      return structClass;
    }

    @Override
    public final Class<V> getValueClass() {
      return valueClass;
    }
  }

  /**
   * Special case of field getter that can get itself.
   *
   * @param <T> The input and ouput type.
   */
  class IdentityFieldGetter<T> extends AbstractFieldGetter<T, T> {
    IdentityFieldGetter(Class<T> structClass) {
      super(structClass, structClass);
    }

    @Override
    public Optional<T> apply(T input) {
      return Optional.ofNullable(input);
    }
  }
}
